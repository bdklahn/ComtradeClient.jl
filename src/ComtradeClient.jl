module ComtradeClient

using URIs, HTTP, JSON3, CodecZlib, Arrow, CSV, DataFrames, CategoricalArrays, StructTypes, Dates
using Glob

export C, S, A, M, H0, H1, H2, H3, H4, H5, H6, SITC, BEC, EBOPS
export pull, get_gen_HS, get_reporters, filter_meta_files, filter_meta_json
export aggregate_intercountry_HS_flow_values_by_period

include("constants.jl")
include("pull.jl")
include("process.jl")

end # ComtradeClient
