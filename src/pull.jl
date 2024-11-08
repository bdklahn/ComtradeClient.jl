# https://comtradeapi.un.org/bulk/v1/get/{typeCode}/{freqCode}/{clCode}[?reporterCode][&period][&publishedDateFrom][&publishedDateTo]
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
    @show headers
    path =
    api === bulk ? j("/", s(a), v, "get", s(typecode), s(freqcode), s(clcode)) : ""

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
    @show uri

    metadata_resp = HTTP.get(uri; headers)
    jsn = JSON3.read(String(metadata_resp.body))

    for d in jsn.data
        hash = "$(d[:rowKey])"

        metadatafile = j(outdir, "$(hash)_metadata.json")
        open(metadatafile, "w") do io
            JSON3.pretty(io, d)
        end

        outfile = j(outdir, "$hash.gz")
        open(outfile, "w") do io
            r_body = HTTP.get(d[:fileUrl], headers, response_stream=IOBuffer()).body
            write(io, take!(r_body))
        end
    end
    jsn
end