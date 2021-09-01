import Base: filter, map, reduce

"""
    map(f, d::DTable) -> DTable

Applies `f` to each row of `d`.
The applied function needs to return a `Tables.Row` compatible object (e.g. `NamedTuple`).

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> m = map(x -> (r = x.a + x.b,), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r = [2, 3, 4],)

julia> m = map(x -> (r1 = x.a + x.b, r2 = x.a - x.b), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(m)
(r1 = [2, 3, 4], r2 = [0, 1, 2])
```
"""
function map(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        if isnonempty(_chunk)
            m = TableOperations.map(_f, _chunk)
            Tables.materializer(_chunk)(m)
        else
            _chunk
        end
    end
    chunks = map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks)
    DTable(chunks, d.tabletype)
end

"""
    reduce(f, d::DTable; cols=nothing, [init]) -> NamedTuple

Reduces `d` using function `f` applied on all columns of the DTable.

By providing the kwarg `cols` as a `Vector{Symbol}` object it's possible
to restrict the reduction to the specified columns.
The reduced values are provided in a NamedTuple under names of reduced columns.

For the `init` kwarg please refer to `Base.reduce` documentation,
as it follows the same principles. 

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> r1 = reduce(+, d)
EagerThunk (running)

julia> fetch(r1)
(a = 6, b = 3)

julia> r2 = reduce(*, d, cols=[:a])
EagerThunk (running)

julia> fetch(r2)
(a = 6,)
```
"""
function reduce(f, d::DTable; cols=nothing::Union{Nothing, Vector{Symbol}}, init=Base._InitialValue())
    # TODO replace this with checking the colnames in schema, once schema handling gets introduced
    if length(d.chunks) > 0
        columns = isnothing(cols) ? Tables.columnnames(Tables.columns(_retrieve(d.chunks[1]))) : cols
    else
        return Dagger.@spawn NamedTuple()
    end
    col_in_chunk_reduce = (_f, _c, _init, _chunk) -> reduce(_f, Tables.getcolumn(_chunk, _c); init=deepcopy(_init))

    chunk_reduce = (_f, _chunk, _cols, _init) -> begin
        # TODO: potential speedup enabled by commented code below by reducing the columns in parallel
        v = [col_in_chunk_reduce(_f, c, _init, _chunk) for c in _cols]
        (; zip(_cols, v)...)

        # TODO: uncomment and define a good threshold for parallelization when this get's resolved
        # https://github.com/JuliaParallel/Dagger.jl/issues/267
        # This piece of code (else option) below is causing the issue above
        # when reduce is repeatedly executed or @btime is used.
        # if length(_cols) <= 1
        #     v = [col_in_chunk_reduce(_f, c, _init, _chunk) for c in _cols]
        # else
        #     values = [Dagger.spawn(col_in_chunk_reduce, _f, c, _init, _chunk) for c in _cols]
        #     v = fetch.(values)
        # end
        # (; zip(_cols, v)...)
    end
    chunk_reduce_results = [Dagger.@spawn chunk_reduce(f, c, columns, deepcopy(init)) for c in d.chunks]

    construct_single_column = (_col, _chunk_results...) -> getindex.(_chunk_results, _col)
    result_columns = [Dagger.@spawn construct_single_column(c, chunk_reduce_results...) for c in columns]

    reduce_result_column = (_f, _c, _init) -> reduce(_f, _c; init=_init)
    reduce_chunks = [Dagger.@spawn reduce_result_column(f, c, deepcopy(init)) for c in result_columns]

    construct_result = (_cols, _vals...) -> (; zip(_cols, _vals)...)
    Dagger.@spawn construct_result(columns, reduce_chunks...)
end



"""
    filter(f, d::DTable) -> DTable

Filter `d` using `f`.
Returns a filtered `DTable` that can be processed further.

# Examples
```julia
julia> d = DTable((a = [1, 2, 3], b = [1, 1, 1]), 2);

julia> f = filter(x -> x.a < 3, d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])

julia> f = filter(x -> (x.a < 3) .& (x.b > 0), d)
DTable with 2 partitions
Tabletype: NamedTuple

julia> fetch(f)
(a = [1, 2], b = [1, 1])
```
"""
function filter(f, d::DTable)
    chunk_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end
    DTable(map(c -> Dagger.spawn(chunk_wrap, c, f), d.chunks), d.tabletype)
end

# temp for continous groupby
function _temp(d::DTable, col; npartitions=-1)
    partition_heuristic = 2 * length(d.chunks)
    if npartitions < 0 
        npartitions = partition_heuristic()
    end
    e = fetch(Dagger.extrema(d, col))

    parts = range(e[1], e[2], length=npartitions)

    chunk_wrap = (chunk, l, r)  -> begin
        if l === nothing
            chunk[(getindex(chunk, :, col) .<= r),:]
        elseif r === nothing
            chunk[(l .< getindex(chunk, :, col)),:]
        else
            chunk[(l .< getindex(chunk, :, col)) .& (getindex(chunk, :, col) .<= r),:]
        end
    end
    v = Vector{Dagger.EagerThunk}()
    

    intervals = [
        (nothing, parts[begin + 1]),
        [(parts[i], parts[i + 1]) for i in 2:length(parts) - 2]...,
        (parts[end-1], nothing)
    ]

    index = Dict{eltype(intervals), Vector{Int}}()


    for i in intervals
        l = length(v)
        append!(v, [Dagger.@spawn chunk_wrap(c, i[1], i[2]) for c in d.chunks])
        index[i] = collect(l + 1:length(v))
    end
    
    squash = (chunks...) -> vcat(chunks...)
    v2 = Vector{Dagger.EagerThunk}()

    for k in enumerate(keys(index))
        c = getindex.(Ref(v), index[k[2]])
        push!(v2, Dagger.@spawn squash(c...))
        index[k[2]] = [k[1]]
    end

    DTable(v2, d.schema)
end


function groupby(col, d::DTable; merge=true, chunksize=0)
    distinct_values = (_chunk, _col) -> unique(Tables.getcolumn(_chunk, _col))

    filter_wrap = (_chunk, _f) -> begin
        m = TableOperations.filter(_f, _chunk)
        Tables.materializer(_chunk)(m)
    end

    chunk_wrap = (_chunk, _col) -> begin
        vals = distinct_values(_chunk, _col)
        if length(vals) > 1
            [v => Dagger.@spawn filter_wrap(_chunk, x -> Tables.getcolumn(x, _col) .== v) for v in vals]
        else
            [vals[1] => Dagger.@spawn (x->x)(_chunk)]
        end
    end

    v = [Dagger.@spawn chunk_wrap(c, col) for c in d.chunks]

    build_index = (merge, chunksize, vs...) -> begin
        v = vcat(vs...)
        ks = unique(map(x-> x[1], v))
        chunks = Vector{Union{EagerThunk, Nothing}}(map(x-> x[2], v))
        
        idx = Dict([k => Vector{Int}() for k in ks])
        for (i, k) in enumerate(map(x-> x[1], v))
            push!(idx[k], i) 
        end
        
        if merge && chunksize <= 0 # merge all partitions into one
            sink = Tables.materializer(tabletype(d)())
            v2 = Vector{EagerThunk}()
            sizehint!(v2, length(keys(idx)))
            for (i, k) in enumerate(keys(idx))
                c = getindex.(Ref(chunks), idx[k])
                push!(v2, Dagger.@spawn merge_chunks(sink, c...))
                idx[k] = [i]
            end
            idx, v2
        elseif merge && chunksize > 0 # merge all but keep the chunking approximately at chunksize with minimal merges
            sink = Tables.materializer(tabletype(d)())
            for (i, k) in enumerate(keys(idx))
                _indices = idx[k]
                _chunks = getindex.(Ref(chunks), _indices)
                _lengths = fetch.(Dagger.spawn.(rowcount, _chunks))
                c = collect.(collect(zip(_indices, _chunks, _lengths)))
                sort!(c, by=(x->x[3]), rev=true)

                l = 1
                r = length(c)

                while l < r
                    if c[l][3] >= chunksize
                        l += 1
                    elseif c[l][3] + c[r][3] > chunksize
                        l += 1
                    elseif c[l][3] + c[r][3] <= chunksize # merge
                        c[l][2] = Dagger.@spawn merge_chunks(sink, c[l][2], c[r][2])
                        c[l][3] = c[l][3] + c[r][3]
                        r -= 1
                    end
                end
                @assert l == r
                for i in 1:length(c)
                    if i <= l
                        chunks[c[i][1]] = c[i][2]
                    else
                        chunks[c[i][1]] = nothing
                    end
                end
                idx[k] = map(x-> x[1], c[1:l])
            end
            idx, filter(x-> !isnothing(x), chunks)
        else
            idx, chunks
        end
    end

    res = Dagger.@spawn build_index(merge, chunksize, v...)
    r = fetch(res)
    DTable(Vector{EagerThunk}(r[2]), d.tabletype, Dict(col => r[1])) 
end

merge_chunks(sink, chunks...) = sink(TableOperations.joinpartitions(Tables.partitioner(x -> x, chunks)))

rowcount(chunk) = length(Tables.rows(chunk))

