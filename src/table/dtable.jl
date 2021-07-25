import DataFrames
import Tables

import Base: fetch, filter, map

const VTYPE = Vector{Union{Dagger.Chunk,Dagger.EagerThunk}}

struct DTable
    chunks::VTYPE

    DTable(chunks::VTYPE) = new(chunks)
    DTable(chunks::Vector{Dagger.EagerThunk}) = new(VTYPE(chunks))
    DTable(chunks::Vector{Dagger.Chunk}) = new(VTYPE(chunks))
end

function DTable(table; chunksize=10_000)
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
    return DTable(chunks)
end

function DTable(files::Vector{String}, loader_function)
    chunks = Vector{Dagger.Chunk}()
    sizehint!(chunks, length(files))

    _load = file -> loader_function(file)
    create_chunk = (rows) -> begin 
        df = DataFrames.DataFrame(rows)
        return Dagger.tochunk(df)
    end

    push!.(Ref(chunks), create_chunk.(_load.(files)))
    return DTable(chunks)
end
    
function filter(f, d::DTable)
    _f = x -> Dagger.@spawn filter(f, x)
    DTable(map(_f, d.chunks))
end

function fetch(d::DTable)
    vcat(_retrieve.(d.chunks)...)
end

_retrieve(x::Dagger.EagerThunk) = fetch(x)
_retrieve(x::Dagger.Chunk) = collect(x)

function fetchcolumn(d::DTable, s::Symbol)
    _f = (x) -> Dagger.@spawn getindex(x, :, s)
    fetch(DTable(map(_f, d.chunks)))
end

function map(f, d::DTable)
    thunk_f = x -> Dagger.@spawn eachrow(x) # eachrow baked in here for now
    row_f = x -> Dagger.@spawn map(f, thunk_f(x))
    DTable(map(row_f, d.chunks))
end

export DTable
