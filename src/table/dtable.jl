import Tables
import DataFrames

import Base: fetch, filter

mutable struct DTable
    v::Vector{Dagger.EagerThunk}
end


function DTable(table::DataFrames.DataFrame)
    rows = Tables.rows(table)
    n = length(rows)
    return DTable([Dagger.@spawn DataFrames.DataFrame(rows[i:(i+1)%(n+1)]) for i in 1:2:n])
end


function getrow(d::DTable, row)
    return Dagger.@spawn getindex(d.v[1], row, :)
end

function apply(f, d::DTable)
   

    v = map(x-> Dagger.spawn(f, (x)), d.v)
    return DTable(v)
end



function fetch(d::DTable)
    vcat(fetch.(d.v)...)
end

export DTable, getrow, apply
