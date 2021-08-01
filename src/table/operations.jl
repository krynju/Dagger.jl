import Base: filter, map, reduce

"""
    function map(f, d::DTableRows)

Applies `f` to each row of the `DTable`.

# Examples
```jldoctest        
    d = DTable((a=rand(1000), b=rand(1000));chunksize=100)
    m = map(x -> x.a + x.b, eachrow(d))
    fetch(m)
```
"""
function map(f, d::DTableRows)
    chunk_wrap = chunk -> begin 
        i = eachrow(chunk)
        map(f, i)    
    end
    result_vector = [Dagger.@spawn chunk_wrap(c) for c in d.dtable.chunks]
    Dagger.@spawn ((r...) -> vcat((r)...))(result_vector...)
end

function map(f, d::DTableColumns)
    throw("Not implemented")
end

"""
    function reduce(f, d::DTableRows; init)

Reduces the `DTable` using function `f`.

# Examples
```jldoctest        
    d = DTable((a=rand(1000), b=rand(1000));chunksize=100)
    r = reduce((acc, x) -> acc + x.a, eachrow(d); init=0.0)
    fetch(r)
```
"""
function reduce(f, d::DTableRows; init)
    chunk_wrap = chunk -> begin
        i = eachrow(chunk)
        reduce(f, i; init)
    end
   
    chunk_results = [Dagger.@spawn chunk_wrap(c) for c in d.dtable.chunks]
    
    # this is dumb, probably a better way to do this
    # it gets the function used to aggregate x and y in a reduction function 
    # eg. for: (x,y) -> x + y; this thing will return Base.+ 
    get_aggregate_fun = (fun) -> begin
        ast_f = Base.uncompressed_ast(methods(fun).ms[1])
        return_line = ast_f.code[end]
        if hasproperty(return_line, :val) 
            ast_id_of_ret_val = return_line.val.id # julia 1.6+
        else
            ast_id_of_ret_val = return_line.args[1].id # julia 1.5
        end
        ast_agg_op = ast_f.code[ast_id_of_ret_val].args[1]
        getfield(ast_agg_op.mod, ast_agg_op.name)
    end
    #
    
    reduce_chunk_results = (a...) -> reduce(get_aggregate_fun(f), a; init)

    Dagger.@spawn reduce_chunk_results(chunk_results...)
end

"""
    function filter(f, d::DTable)

Filter the `DTable` using `f`.
Returns a filtered DTable that can be processed further.

# Examples
```jldoctest        
    d = DTable((a=rand(1000), b=rand(1000));chunksize=100)
    f = filter(x -> x.a > 0.5, eachrow(d))
    fetch(f)
```
"""
function filter(f, d::DTable)
    chunk_wrap = chunk -> Dagger.@spawn DataFrames.filter(f, chunk)
    DTable([chunk_wrap(c) for c in d.chunks], d.schema)
end

function select(d::DTable, args...; kwargs...)
    select_wrap = chunk ->  DataFrames.select(chunk, args...; kwargs...)
    chunk_wrap = chunk -> Dagger.@spawn select_wrap(chunk)
    DTable([chunk_wrap(c) for c in d.chunks], nothing)
end

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
