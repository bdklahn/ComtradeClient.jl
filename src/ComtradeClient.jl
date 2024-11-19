module ComtradeClient

using URIs, HTTP, JSON3, CodecZlib, Arrow, CSV, DataFrames, CategoricalArrays, StructTypes, Dates

export pull, get_gen_HS, get_reporters

include("constants.jl")
include("pull.jl")

end # ComtradeClient
