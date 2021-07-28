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
            ast_id_of_ret_val= return_line.args[1].id # julia 1.5
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
    chunk_wrap = chunk -> Dagger.@spawn filter(f, chunk)
    DTable([chunk_wrap(c) for c in d.chunks])
end
