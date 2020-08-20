# Note: Two important features of this code is that it's multithreaded, and that the
# de/compression threads are launched asyncronously, such that it can de/compress one
# block while it's reading other blocks

# Todo: Fix seeking code
# Todo: Fix VirtualOffsets code
# Todo: Fix tests

mutable struct BGZFCodec{T <: DE_COMPRESSOR, O <: IO} <: TranscodingStreams.Codec
	buffer::Vector{UInt8}
	blocks::Vector{Block{T}}

	# We need this in order to write an EOF on end
	io::O

	# Index of currently used block
	index::Int
	bufferlen::Int
end

const CompressorCodec{O} = BGZFCodec{Compressor, O}
const DecompressorCodec{O} = BGZFCodec{Decompressor, O}
const BGZFCompressorStream = TranscodingStream{CompressorCodec}
const BGZFDecompressorStream = TranscodingStream{DecompressorCodec}

function BGZFCompressorStream(io::IO; nthreads=Threads.nthreads(), compresslevel::Int=6)
    codec = CompressorCodec(io, nthreads, compresslevel)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Compressor}))
end

function BGZFDecompressorStream(io::IO; nthreads=Threads.nthreads())
    codec = DecompressorCodec(io, nthreads)
    return TranscodingStream(codec, io; bufsize=nfull(Block{Decompressor}))
end

function CompressorCodec(io::IO, nthreads, compresslevel)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
    buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Compressor(compresslevel)) for i in 1:nthreads]
    return CompressorCodec{typeof(io)}(buffer, blocks, io, 1, 0)
end

function DecompressorCodec(io::IO, nthreads)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
	buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Decompressor()) for i in 1:nthreads]
	return DecompressorCodec{typeof(io)}(buffer, blocks, io, 1, 0)
end

function CompressorCodec(io::IO, nthreads, compresslevel)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
    buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Compressor(compresslevel)) for i in 1:nthreads]
    return CompressorCodec{typeof(io)}(buffer, blocks, io, 1, 0)
end

function DecompressorCodec(io::IO, nthreads)
	nthreads < 1 && throw(ArgumentError("Must use at least 1 thread"))
	buffer = Vector{UInt8}(undef, MAX_BLOCK_SIZE)
    blocks = [Block(Decompressor()) for i in 1:nthreads]
	return DecompressorCodec{typeof(io)}(buffer, blocks, io, 1, 0)
end

nblocks(c::BGZFCodec) = length(c.blocks)
get_block(c::BGZFCodec) = @inbounds c.blocks[c.index]
remaining(codec::BGZFCodec{T}) where T = nfull(Block{T}) - codec.bufferlen

"Returns the index of the next nonempty block, or nothing if no block is"
function next_block_index(codec::BGZFCodec)
	i = ifelse(codec.index == nblocks(codec), 1, codec.index + 1)

	# We need to wait for the blocks to make sure they're actually empty
	wait(codec.blocks[i])
	while isempty(codec.blocks[i])
		i == codec.index && return nothing
		i = ifelse(i == nblocks(codec), 1, i + 1)
		wait(codec.blocks[i])
	end
	return i
end

function last_offset(codec::BGZFCodec)
    i = codec.index - 1
	i = ifelse(iszero(i), nblocks(codec), i)
	block = @inbounds codec.blocks[i]
	return block.offset + block.blocklen
end

function reset!(s::BGZFDecompressorStream)
    TranscodingStreams.initbuffer!(s.state.buffer1)
    TranscodingStreams.initbuffer!(s.state.buffer2)
	for block in s.blocks
		empty!(block)
	end
	s.index = 0
	s.bufferlen = 0
    return s
end

function Base.seek(s::BGZFDecompressorStream, i::Integer)
    seek(s.stream, i)
    reset!(s)
    last(s.blocks).offset = i
    return s
end

function Base.seekstart(s::BGZFDecompressorStream)
    seekstart(s.stream)
    reset!(s)
    last(s.blocks).offset = 0
end

function Base.seek(s::BGZFDecompressorStream, v::VirtualOffset)
    block_offset, byte_offset = offsets(v)
    seek(s, block_offset)

    # Read one byte to fill in buffer
    read(s, UInt8)

    # Now advance buffer block_offset minus the one byte we just read
    if byte_offset ≥ first(s.codec.blocks).decompress_len
        throw(ArgumentError("Too large offset for block"))
    end
    s.state.buffer1.bufferpos += (byte_offset % Int - 1)
    return s
end

function VirtualOffset(s::BGZFDecompressorStream)
    # Loop over blocks, adding up the decompress_len. When that surpasses s.outpos,
    # we have the right block. We also get the offset within the block this way.
    # With the block, we can iterate over all block.blocksize to get the block
    # offset.
    decompress_len = 0
    inblock_offset = 0
    block_offset = first(s.codec.offset)
    buffer_offset = s.state.buffer1.bufferpos - s.state.buffer1.markpos - 1

    for block in s.codec.blocks
        if decompress_len + block.decompress_len >= buffer_offset
            inblock_offset = buffer_offset - decompress_len
            break
        else
            block_offset += block.blocklen
            decompress_len += block.decompress_len
        end
    end
    return VirtualOffset(block_offset, inblock_offset)
end

function TranscodingStreams.finalize(codec::CompressorCodec)
    write(codec.io, EOF_BLOCK)
end

function copy_from_outbuffer(codec::BGZFCodec, output::Memory, consumed::Integer)
	block = get_block(codec)
	available = block.outlen - block.outpos + 1
    n = min(available, length(output))
    unsafe_copyto!(output.ptr, pointer(block.outdata, block.outpos), n)
	block.outpos += n
    return (Int(consumed), n, :ok)
end

function TranscodingStreams.process(codec::BGZFCodec{T}, input::Memory, output::Memory, error::Error) where T
	return _process(codec, input, output, error, nfull(Block{T}))
end

function _process(codec::BGZFCodec, input, output, error, blocksize)
	consumed = 0
    block = get_block(codec)

	# If we have spare data in the current block, just give that
    isempty(block) || return copy_from_outbuffer(codec, output, consumed)

	# If there is data to be read in, we do that.
	if !iszero(length(input))
    	consumed = min(remaining(codec), length(input))
    	unsafe_copyto!(pointer(codec.buffer, codec.bufferlen + 1), input.ptr, consumed)
    	codec.bufferlen += consumed

		# If we have read in data, but still not enough to queue a block, return no data
		# and wait for more data to be passed
    	codec.bufferlen < blocksize && return (consumed, 0, :ok)
    end

    # At this point, if there is any data in the buffer, it must be enough
    # to queue a whole block
    if !iszero(codec.bufferlen)
    	indexed = index!(block, codec.buffer, get_offset(codec), codec.bufferlen)
    	codec.bufferlen -= indexed
    	queue!(block)
    end

    # If there is no more data, we need to go to next nonempty block and retrieve
    # data from there. This might not be the next block, in case of short files
	if iszero(length(input))
    	index = next_block_index(codec)
    	if index === nothing
    		codec isa DecompressorCodec && check_eof_block(block)
    		return (consumed, 0, :end)
    	end
    	codec.index = index

    # If there is more data, we either have data in next block to read from,
    # or need to load data into the next block anyway
    else
        index = ifelse(codec.index == nblocks(codec), 1, codec.index + 1)
	    codec.index = index
	    wait(codec.blocks[index])
	    return (consumed, 0, :ok)
    end
    # Return data from this new block
    return copy_from_outbuffer(codec, output, consumed)
end

