using Test
using DataFrames
using Arrow
using CSV

@testset "dtable" begin
    @testset "constructors - Tables.jl compatibility (Vector{NamedTuple})" begin
        size = 1_000
        d = DTable([(a = 10, b = 20) for i in 1:size]; chunksize=100)
        @test fetch(d) == DataFrame([(a = 10, b = 20) for i in 1:size])
    end

    @testset "constructors - Tables.jl compatibility (DataFrames)" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        @test fetch(d) == s
        end

    @testset "constructors - Tables.jl compatibility (Arrow)" begin
        size = 1_000
        io = IOBuffer()
        Arrow.write(io, [(a = 10, b = 20) for i in 1:size])
        t = Arrow.Table(take!(io))
        d = DTable(t; chunksize=100)
        @test fetch(d) == DataFrame(t)
    end

        @testset "constructors - Tables.jl compatibility (CSV)" begin
        size = 15_000
        io = IOBuffer()
        CSV.write(io, [(a = 10, b = 20) for i in 1:size])
        d = CSV.read(take!(io), DTable)
        CSV.write(io, [(a = 10, b = 20) for i in 1:size])
        # or
        d2 = DTable(CSV.File(take!(io)); chunksize=1_000)
        @test fetch(d) == DataFrame([(a = 10, b = 20) for i in 1:size])
        @test fetch(d2) == DataFrame([(a = 10, b = 20) for i in 1:size])
    end

    @testset "constructors - file input" begin
        n = 20
        size = 1000
        ios = [IOBuffer() for _ in 1:n]
        data = [(a = rand(size), b = rand(size)) for _ in 1:n]
        arr = [Arrow.write(ios[idx], data[idx]) for idx in 1:n]

        da = DTable([string(i) for i in 1:n], (x) -> Arrow.Table(take!(ios[tryparse(Int64, x)])))
        db = vcat([DataFrame(d) for d in data]...)
        @test fetch(da) == db
    end

    @testset "map" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        sm =  map(x -> x.a + x.b, eachrow(s))
        dm = fetch(map(x -> x.a + x.b, eachrow(d)))
        @test sm == dm
    end

    @testset "filter" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s; chunksize=100)
        s_r = filter(x -> x.a > 0.5, s)
        d_r = fetch(filter(x -> x.a > 0.5, d))
        @test s_r == d_r
    end

    @testset "reduce" begin
        size = 1_000
        sample_df = () -> DataFrame(a=rand(Int, size).%1000, b=rand(Int, size).%1000)
        s = sample_df()
        d = DTable(s; chunksize=100)

        dr1 = reduce((x,y)-> x + y.a, eachrow(d);init=0)
        sr1 = reduce((x,y)-> x + y.a, eachrow(s);init=0)
        @test fetch(dr1) == sr1

        dr2 = reduce((x,y)-> x + y.a * y.b, eachrow(d);init=0)
        sr2 = reduce((x,y)-> x + y.a * y.b, eachrow(s);init=0)
        @test fetch(dr2) == sr2

        dr3 = reduce((x,y)-> x + y.a + y.b, eachrow(d);init=0)
        sr3 = reduce((x,y)-> x + y.a + y.b, eachrow(s);init=0)
        @test fetch(dr3) == sr3

        dr4 = reduce((x,y)-> x * y.a, eachrow(d);init=0)
        sr4 = reduce((x,y)-> x * y.a, eachrow(s);init=0)
        @test fetch(dr4) == sr4
    end
end
