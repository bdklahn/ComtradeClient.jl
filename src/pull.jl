# to only extract the id and text of the results in H6.json
struct H6Result
    id::String
    text::String
end

struct H6
    results::Array{H6Result, 1}
end

StructTypes.StructType(::Type{H6Result}) = StructTypes.Struct()
StructTypes.StructType(::Type{H6}) = StructTypes.Struct()

categ_compress(v) = categorical(v, compress = true)

"""
Get the H6 classifications and generate a
feather id to text lookup table
"""
function get_gen_H6(outdir::String="./")
    jsn_str = String(HTTP.get(H6_json_url).body)
    jsn = JSON3.read(jsn_str)
    open(joinpath(outdir, "H6.json"), "w") do io
        JSON3.pretty(io, jsn)
    end
    h6_results = JSON3.read(jsn_str, H6).results
    df = DataFrame(id = [r.id for r in h6_results], text = [r.text for r in h6_results])
    transform!(df, [:id, :text] .=> categ_compress, renamecols=false)
    open(joinpath(outdir, "H6_id_text.feather"), "w") do io
        Arrow.write(io, df)
    end
    jsn
end

"""
Generate standard URI's for Comtrade endpoints.
"""
function pull(domain::String=domain;
    api::API=bulk,
    typecode::typeCode=C,
    freqcode::freqCode=M,
    clcode::clCode=HS,
    reportercode::String="",
    period::String="",
    publisheddatefrom::String="",
    publisheddateto::String="",
    subscription_key::String=get(ENV, "COMTRADE_API_KEY", ""),
    outdir::String="./",
    )
    j, a, s = joinpath, string(api), string

    headers=Vector{Pair{String, String}}()
    if !isempty(subscription_key) push!(headers, "Ocp-Apim-Subscription-Key" => subscription_key) end
    path = api === bulk ? j("/", s(a), v, "get", s(typecode), s(freqcode), s(clcode)) : "/"

    query = []
    for (n, q) in (
        ("reporterCode", reportercode),
        ("period", period),
        ("publishedDateFrom", publisheddatefrom),
        ("publishedDateTo", publisheddateto),
        )
        if !isempty(q) push!(query, "$n=$q") end
    end

    query = join(query, "&")

    uri = URI(;scheme="https", host=domain, path, query)
    @info uri

    metadata_json = JSON3.read(String(HTTP.get(uri; headers).body))

    for d in metadata_json.data
        hash = "$(d[:rowKey])"

        metadatafile = j(outdir, "$(hash)_metadata.json")
        open(metadatafile, "w") do io
            JSON3.pretty(io, d)
        end

        outfile = j(outdir, "$hash.feather")
        open(outfile, "w") do io
            r = HTTP.get(d[:fileUrl], headers, response_stream=IOBuffer())
            df = CSV.read(transcode(GzipDecompressor, take!(r.body)), DataFrame; downcast=true)
            transform!(df, categoricals .=> categ_compress, renamecols=false)
            Arrow.write(io, df)
        end
    end
    metadata_json
end