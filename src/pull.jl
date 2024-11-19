# to only extract the id and text of the results in H6.json
struct HSResult
    id::String
    text::String
end

struct HSroot
    results::Array{HSResult, 1}
end

StructTypes.StructType(::Type{HSResult}) = StructTypes.Struct()
StructTypes.StructType(::Type{HSroot}) = StructTypes.Struct()

categ_compress(v) = categorical(v, compress = true)

"""
Get the H6 classifications and generate a
feather id to text lookup table
"""
function get_gen_HS(;
    outdir::String="./",
    version::String="H6",
    writejson::Bool=true,
    writearrow::Bool=true,
    )
    jsn = JSON3.read(HTTP.get(HS_json_url * "$version.json").body)

    if writejson
        open(joinpath(outdir, "H6.json"), "w") do io
            JSON3.pretty(io, jsn)
        end
    end
    if !writearrow return jsn end
    h6_results = JSON3.read(JSON3.write(jsn), HSroot).results
    df = DataFrame(id = [r.id for r in h6_results], text = [r.text for r in h6_results])
    transform!(df, [:id, :text] .=> categ_compress, renamecols=false)
    open(joinpath(outdir, "H6_id_text.feather"), "w") do io
        Arrow.write(io, df; file=true)
    end
    jsn
end

"""
Get the json file which encodes the reporters information,
download, and generate an Arrow lookup table.
"""
function get_reporters(
    url::String=reporters_url;
    outdir::String="./",
    writejson::Bool=true,
    writearrow::Bool=true,
    )
    jsn = JSON3.read(HTTP.get(url).body)
    if writejson
        open(joinpath(outdir, "Reporters.json"), "w") do io
            JSON3.pretty(io, jsn)
        end
    end

    if !writearrow return jsn end

    jres = jsn.results
    df = DataFrame(
        id = [r.id for r in jres],
        text = [r.text for r in jres],
        reporterCode = [r.reporterCode for r in jres],
        reporterDesc = [r.reporterDesc for r in jres],
        reporterNote = [hasproperty(r, :reporterNote) ? r.reporterNote : "" for r in jres],
        reporterCodeIsoAlpha2 = [hasproperty(r, :reporterCodeIsoAlpha2) ? r.reporterCodeIsoAlpha2 : "" for r in jres],
        reporterCodeIsoAlpha3 = [r.reporterCodeIsoAlpha3 for r in jres],
        entryEffectiveDate = [r.entryEffectiveDate for r in jres],
        isGroup = [r.isGroup for r in jres],
        )
    transform!(df, [:reporterCode, :reporterCodeIsoAlpha2, :reporterCodeIsoAlpha3] .=> categ_compress, renamecols=false)
    df.entryEffectiveDate = DateTime.(df.entryEffectiveDate)
    df.isGroup = Bool.(df.isGroup)
    df.id = UInt16.(df.id)
    open(joinpath(outdir, "Reporters.feather"), "w") do io
        Arrow.write(io, df; file=true)
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
    nfileslimit::Union{Int, Nothing}=nothing,
    overwriteexisting::Bool=false,
    )
    mkpath(outdir)
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

    """
    json array containing metadata about each file (including the download url)"
    `nothing` means no limit is applied, and thus all available files will be
    downloaded.
    """
    filesmetaarray = nfileslimit === nothing ? metadata_json.data : metadata_json.data[1:nfileslimit+1]

    for d in filesmetaarray
        hash = "$(d[:rowKey])"

        outfile = j(outdir, "$hash.feather")
        if isfile(outfile) && !overwriteexisting
            @warn """
            skipping $outfile
            . . . because it already exists.
            Set `overwriteexisting` to `true` to force overwrite.
            """
            continue
        end

        open(outfile, "w") do io
            r = HTTP.get(d[:fileUrl], headers, response_stream=IOBuffer())
            df = CSV.read(transcode(GzipDecompressor, take!(r.body)), DataFrame; downcast=true)
            transform!(df, categoricals .=> categ_compress, renamecols=false)
            @show outfile
            Arrow.write(io, df; file=true)
        end
    end
    open(j(outdir, "meta.json"), "w") do io
        JSON3.pretty(io, metadata_json)
    end
    metadata_json
end