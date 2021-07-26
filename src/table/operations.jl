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
    
    # this is dumb, probably a better way to do this
    # it gets the function used to aggregate x and y in a reduction function 
    # eg. for: (x,y) -> x + y; this thing will return Base.+ 
    return_line = Base.uncompressed_ast(methods(f).ms[1]).code[end]
    if hasproperty(return_line, :val) 
        ast_id_of_ret_val = return_line.val.id # julia 1.6+
    else
        ast_id_of_ret_val= return_line.args[1].id # julia 1.5
    end
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
