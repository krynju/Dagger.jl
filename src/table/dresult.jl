struct DResult
    chunks::Vector{Dagger.EagerThunk}
    chunk_aggregate_function::Function
    aggregated_result::Union{Nothing,Dagger.EagerThunk} = nothing
end

function fetch(d::DResult)
    if aggregated_result === nothing
        aggregate(d)
    end
    return fetch(d.aggregated_result)
end

function aggregate(d::DResult)
    wrap = (chunks...) -> reduce(d.chunk_aggregate_fun, chunks)
    d.aggregated_result = Dagger.@spawn wrap(d.chunks...)
end