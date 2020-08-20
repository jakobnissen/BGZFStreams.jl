const BLOCK_HEADER = [
  # ID1   ID2   CM    FLG   |<--     MTIME    -->|
    0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  # XFL   OS    |  XLEN  |  S1    S2    |  SLEN  |
    0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00
]

# end-of-file marker block (used for detecting unintended file truncation)
const EOF_BLOCK = vcat(BLOCK_HEADER, [
  # |  BSIZE |  |  DATA  |  |        CRC32       |
    0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  # |        ISIZE       |
    0x00, 0x00, 0x00, 0x00
])

# BGZF blocks are no larger than 64 KiB before and after compression.
const MAX_BLOCK_SIZE = UInt(64 * 1024)

# Maximum number of bytes to be compressed at one time. Random bytes usually end up filling
# a bit more when compressed, so we have a 256 byte margin of safety.
const SAFE_BLOCK_SIZE = UInt(MAX_BLOCK_SIZE - 256)

mutable struct Block{T}
	de_compressor::T
	outdata::Vector{UInt8}
	indata::Vector{UInt8}
	task::Task
	outlen::Int
	outpos::Int
	inlen::Int
	blocklen::Int
	crc32::UInt32

	# This is the offset of the block in the file it's read from
	offset::Int
end

function Block(dc::T) where T <: DE_COMPRESSOR
	outdata = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
	indata = similar(outdata)

	# We initialize with a trivial, but completable task for sake of simplicity
	task = schedule(Task(() -> nothing))
	return Block{T}(dc, outdata, indata, task, 0, 1, 0, 0, UInt32(0), 0)
end

isempty(block::Block) = block.outpos > block.outlen

function Base.empty!(block::Block)
	block.outlen = 0
	block.outpos = 1
	block.inlen = 0
	block.offset = 0
end

nfull(::Type{Block{Decompressor}}) = MAX_BLOCK_SIZE
nfull(::Type{Block{Compressor}}) = SAFE_BLOCK_SIZE
Base.wait(b::Block) = wait(b.task)

function check_eof_block(block::Block{Decompressor})
    if !iszero(block.inlen)
        bgzferror("No EOF block. Truncated file?")
    end
end

function index!(block::Block{Compressor}, data::Vector{UInt8}, offset::Integer, inlen::Integer)
	copyto!(block.indata, 1, data, 1, inlen)
	block.inlen = inlen
	block.offset = offset
	block.outpos = 1
	return inlen
end

function index!(block::Block{Decompressor}, data::Vector{UInt8}, offset::Integer, inlen::Integer)
	block.outpos = 1
	block.offset = offset
	
    # +---+---+---+---+---+---+---+---+---+---+---+---+
    # |ID1|ID2|CM |FLG|     MTIME     |XFL|OS | XLEN  | (more-->)
    # +---+---+---+---+---+---+---+---+---+---+---+---+
    inlen < 12 && bgzferror("Too small input")
    @inbounds begin
        if !((data[1] == 0x1f) & (data[2] == 0x8b))
            bgzferror("invalid gzip identifier")
        elseif !(data[3] == 0x08)
            bgzferror("invalid compression method")
        elseif !(data[4] == 0x04)
            bgzferror("invalid flag")
        end
    end
    xlen = UInt16(data[11]) | UInt16(data[12]) << 8

    # +=================================+
    # |...XLEN bytes of "extra field"...| (more-->)
    # +=================================+
    inlen < (12 + xlen) && bgzferror("Too small input")
    bsize = UInt16(0) # size of block - 1
    pos = 13
    stop = pos + xlen
    @inbounds while pos < stop
        si1 = data[pos]
        si2 = data[pos+1]
        slen = UInt16(data[pos+2]) | UInt16(data[pos+3]) << 8
        if si1 == 0x42 || si2 == 0x43
            if slen != 2
                bgzferror("invalid subfield length")
            end
            bsize = bitload(UInt16, data, pos+4)
        end
        # skip this field
        pos += 4 + slen
    end
    if bsize == 0
        bgzferror("no block size")
    end

	# Size of compressed data
    block.inlen = bsize - xlen - 19

    # +=======================+---+---+---+---+---+---+---+---+
    # |...compressed blocks...|     CRC32     |     ISIZE     |
    # +=======================+---+---+---+---+---+---+---+---+
    block.blocklen = bsize + 1
    inlen < block.blocklen && bgzferror("Too small input")

	block.crc32 = bitload(UInt32, data, block.blocklen - 7)
    block.outlen = bitload(UInt32, data, block.blocklen - 3)

    # Move data
    copyto!(block.indata, 1, data, 13 + xlen, block.inlen)

    # Shift remaining data in input buffer if we didn't comsume everything
    copyto!(data, 1, data, block.blocklen+1, inlen - block.blocklen)
    return block.blocklen
end

# This does the full transformation from input to output data
queue!(block::Block) = block.task = @spawn _queue!(block)

function _queue!(block::Block{Compressor})
	# Meat: The compressed data
	compress_len = unsafe_compress!(block.de_compressor,
                   pointer(block.outdata, 19), MAX_BLOCK_SIZE - 26,
                   pointer(block.indata), block.inlen)
    block.crc32 = unsafe_crc32(pointer(block.indata), block.inlen)
    block.outlen = compress_len + 26

    # Header: 18 bytes of header
    unsafe_copyto!(block.outdata, 1, BLOCK_HEADER, 1, 16)
	bitstore(UInt16(block.outlen - 1), block.outdata, 17)

    # Tail: CRC + isize
	bitstore(block.crc32, block.outdata, 18 + compress_len + 1)
	bitstore(block.inlen % UInt32, block.outdata, 18 + compress_len + 5)
	
    block.inlen = 0
end

function _queue!(block::Block{Decompressor})
    unsafe_decompress!(Base.HasLength(), block.de_compressor,
                       pointer(block.outdata), block.outlen,
                       pointer(block.indata), block.inlen)

    crc32 = unsafe_crc32(pointer(block.outdata), block.outlen)
    crc32 != block.crc32 && bgzferror("CRC32 checksum does not match")
	block.inlen = 0
end
