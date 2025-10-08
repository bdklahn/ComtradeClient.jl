"""
# Introduction
Aggregate bulk data values by period and counties for 6-digit HS codes.

# Arguments
- `datapath`: Directory containing the data files

# Options
- `freqcode`: Frequency code, either annual (`A`) or monthly (`M`). Default is annual.

"""
function aggregate_intercountry_HS_flow_values_by_period(
  datapath::String;
  freqcode::freqCode=A,
  filelimit::Int=99999,
  sampleevery::Int=1,
  from::Int=200001,
  to::Int=year(now())*100 + month(now()),
)
  @assert to >= from && from >= 200001 && to <= year(now())*100 + month(now()) "from and to must be in YYYYMM format and within valid range"
  meta = JSON3.read(joinpath(datapath, "meta.json"))
  files = [joinpath(datapath, "$(d.rowKey).arrow") for d in meta.data if (from <= d.period <= to)]
  files = files[1:min(filelimit, length(files))]
  files = files[1:sampleevery:end]
  @info "number of files" length(files)
  periodcol = freqcode == A ? :refYear : :period
  @info """
         time period column to aggregate by:
         Use year, otherwise use period (month resolution)
         """ periodcol
  df = DataFrame(Arrow.Table(files))[!, [:partnerCode, :reporterCode, :cmdCode, periodcol, :primaryValue, :flowCode]]
  @info "done loading subset dataframe"
  df = @view df[(df.flowCode.=="M").&(length.(df.cmdCode).==6).&(df.partnerCode.!==0).&(completecases(df)), :]
  @info "done filtering dataframe"
  gdf = groupby(df, [:partnerCode, :reporterCode, :cmdCode, periodcol])
  @info "done grouping dataframe"
  df = combine(gdf, :primaryValue => sum => :value)
end
