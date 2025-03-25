module ComtradeClient

using URIs, HTTP, JSON3, CodecZlib, Arrow, CSV, DataFrames, CategoricalArrays, StructTypes, Dates

export C, S, A, M, H0, H1, H2, H3, H4, H5, H6, SITC, BEC, EBOPS
export pull, get_gen_HS, get_reporters, filter_meta_files, filter_meta_json

include("constants.jl")
include("pull.jl")

end # ComtradeClient
