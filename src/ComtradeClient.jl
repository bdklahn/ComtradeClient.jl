module ComtradeClient

using URIs, HTTP, JSON3

export pull

include("constants.jl")
include("pull.jl")

end # ComtradeClient
