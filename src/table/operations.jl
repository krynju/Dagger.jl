import Base: filter, map, reduce

function map(f, d::DTableRows)
    chunk_wrap = chunk -> begin 
        i = eachrow(chunk)
        map(f, i)    
    end
    DTable([Dagger.@spawn chunk_wrap(c) for c in d.dtable.chunks])
end

function map(f, d::DTableColumns)
    throw("Not implemented")
end

function reduce(f, d::DTableRows; init)
    chunk_wrap = chunk -> begin
        i = eachrow(chunk)
        reduce(f, i; init=init)
    end
   
    chunk_results = [Dagger.@spawn chunk_wrap(c) for c in d.dtable.chunks]
    
    # this is dumb
    # it gets the function used to aggregate x and y in a reduction function 
    # eg. for: (x,y) -> x + y; this thing will return Base.+ 
    ast_id_of_ret_val = Base.uncompressed_ast(methods(f).ms[1]).code[end].val.id
    ast_agg_op = Base.uncompressed_ast(methods(f).ms[1]).code[ast_id_of_ret_val].args[1]
    chunk_aggregate_fun = getfield(ast_agg_op.mod, ast_agg_op.name)
    #
    
    reduce_chunk_results = (a...) -> reduce(chunk_aggregate_fun, a;init)

    Dagger.@spawn reduce_chunk_results(chunk_results...)
end

function filter(f, d::DTable)
    chunk_wrap = chunk -> Dagger.@spawn filter(f, chunk)
    DTable([chunk_wrap(c) for c in d.chunks])
end
