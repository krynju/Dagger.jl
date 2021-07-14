using Test
using DataFrames

@testset "dtable" begin

    @testset "constructors" begin
        size = 10
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s)
        @test fetch(d) == s
    end

    @testset "map" begin
        size = 10
        sample_df = () -> DataFrame(a=rand(size), b=rand(size))
        s = sample_df()
        d = DTable(s)
        @test map(x-> x.a + x.b, eachrow(s)) == fetch(map(x-> x.a + x.b, d))
    end

end