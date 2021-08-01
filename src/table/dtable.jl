import .DataFrames
import Tables

import Base: fetch, extrema

const VTYPE = Vector{Union{Dagger.Chunk,Dagger.EagerThunk}}

"""
    struct DTable

Structure representing the distributed table based on Dagger.

The table is stored as a vector of `Chunk` structures which hold partitions of the table.
That vector can also store `EagerThunk` structures when an operation that modifies
the underlying partitions was applied to it (currently only `filter`).

Underlying partitions should always be `DataFrame` structures.
"""
mutable struct DTable
    chunks::VTYPE
    schema::Union{Nothing,Tables.Schema}

    DTable(chunks::VTYPE) = new(chunks, nothing)
    DTable(chunks::VTYPE, schema::Tables.Schema) = new(chunks, schema)
    DTable(chunks::Vector{Dagger.EagerThunk}, args...) = new(VTYPE(chunks), args...)
    DTable(chunks::Vector{Dagger.Chunk}, args...) = new(VTYPE(chunks), args...)
end

include("iterators.jl")
include("operations.jl")

""" 
    function DTable(table; chunksize)

Constructs a `DTable` using a `Tables.jl` compatible `table` input.
It assumes no initial partitioning of the table and uses the `chunksize`
keyword argument to partition the table (based on row count).
"""
function DTable(table; chunksize)
    if !Tables.istable(table)
        throw(ArgumentError("Provided input is not Tables.jl compatible."))
    end
    create_chunk = (rows) -> begin 
        df = DataFrames.DataFrame(rows)
        return Dagger.tochunk(df)
    end
    chunks = Vector{Dagger.Chunk}()

    it = Tables.rows(table)
    buffer = Vector{eltype(it)}()
    sizehint!(buffer, chunksize)
    p = iterate(it)
    counter = 0

    while !isnothing(p) 
        push!(buffer, p[1])
        counter += 1
        p = iterate(it, p[2])
        if counter == chunksize
            push!(chunks, create_chunk(buffer))
            empty!(buffer)
            counter = 0 
        end
    end
    if counter > 0
    push!(chunks, create_chunk(buffer))
        empty!(buffer)
    end
        
    schema = Tables.schema(it)
    return DTable(chunks, schema)
end

"""
    function DTable(files::Vector{String}, loader_function)

Constructs a `DTable` using a list of filenames and a `loader_function`.
Partitioning is based on the contents of the files provided, which means that
one file is used to create one partition.

# Examples
```jldoctest
    DTable(["a.csv", "b.csv"], CSV.File)
    DTable(["a.arrow", "b.arrow"], Arrow.Table)
```
"""
function DTable(files::Vector{String}, loader_function)
    chunks = Vector{Dagger.Chunk}()
    sizehint!(chunks, length(files))

    schema = nothing

    _load = file -> loader_function(file)
    create_chunk = (rows) -> begin 
        if schema === nothing 
            schema = Tables.schema(rows)
        elseif schema != Tables.schema(rows)
            throw(ArgumentError("Files provided as input have various schemas. Currently not supported."))
        end
        df = DataFrames.DataFrame(rows)
        return Dagger.tochunk(df)
    end

    push!.(Ref(chunks), create_chunk.(_load.(files)))
    return DTable(chunks, schema)
end

"""
    function fetch(d::DTable)

Fetches all the chunks and constructs the full `DTable` in memory as a `DataFrame` structure.
"""
function fetch(d::DTable)
    r = _retrieve.(d.chunks)
    @assert all(isa.(r, DataFrames.DataFrame))
    vcat(r...)
end

_retrieve(x::Dagger.EagerThunk) = fetch(x)
_retrieve(x::Dagger.Chunk) = collect(x)

function getcolumn(d::DTable, s::Symbol)
    _f = (x) -> Dagger.@spawn getindex(x, :, s)
    Dagger.@spawn ((r...) -> vcat((r)...))(map(_f, d.chunks)...)
end

function Tables.schema(d::DTable)
    if d.schema === nothing
        anychunk = d.chunks[begin]
        s = Dagger.@spawn Tables.schema(anychunk)
        d.schema = fetch(s)
    end
    return d.schema
end

function extrema(d::DTable, col)
    chunk_wrap = chunk -> extrema(getindex(chunk, :, col))
    extremas = [Dagger.@spawn chunk_wrap(c) for c in d.chunks]
    extremas_wrap = (e...) -> begin
        _min = minimum(getindex.(e, 1))
        _max = maximum(getindex.(e, 2))
        (_min, _max)
    end
    Dagger.@spawn extremas_wrap(extremas...)
end
    
export DTable
