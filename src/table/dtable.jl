import DataFrames
import Tables

import Base: fetch, filter, map, setproperty!

mutable struct DTable
    v::Vector{Dagger.EagerThunk}
    cols::Vector{Symbol} 
    schema::Tables.Schema
end



function DTable(v::Vector{Dagger.EagerThunk})
    names = Tables.columnnames(fetch(v[begin]))
    cols = collect(Symbol, names)
    schema = Tables.Schema(names, nothing)
    DTable(v, cols, schema)
end


function DTable(table::DataFrames.DataFrame; thunksize=10)
    rows = Tables.rows(table)
    n = length(rows)
    v = [Dagger.@spawn DataFrames.DataFrame(rows[i:(i + thunksize - 1) % (n + 1)]) for i in 1:thunksize:n]
    return DTable(v)
end

function filter(f, d::DTable)
    _f = x -> Dagger.@spawn filter(f, x)
    DTable(map(_f, d.v))
end

function fetch(d::DTable)
    _fetch_thunk_vector(d.v)
end

function _fetch_thunk_vector(x)
    vcat(fetch.(x)...)
end

function fetchcolumn(d::DTable, s::Symbol)
    _f = (x) -> Dagger.@spawn getindex(x, :, s)
    _fetch_thunk_vector(map(_f, d.v))
end

function map(f, d::DTable)
    thunk_f = x -> Dagger.@spawn eachrow(x)
    row_f = x -> Dagger.@spawn map(f, thunk_f(x))
    DTable(map(row_f, d.v))
end

export DTable, getrow, apply, fetchcolumn
