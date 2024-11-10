module ComtradeClient

using URIs, HTTP, JSON3, CodecZlib, Arrow, CSV, DataFrames, CategoricalArrays, StructTypes

export pull, get_gen_H6

include("constants.jl")
include("pull.jl")

end # ComtradeClient
