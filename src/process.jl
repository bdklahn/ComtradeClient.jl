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
)
    files = Glob.glob("*.arrow", datapath)[1:33:end]
    "time period column to aggregate by: Use year, otherwise use period (month resolution)"
    periodcol = freqcode == A ? :refYear : :period
    df = DataFrame(Arrow.Table(files))[!, [:partnerCode, :reporterCode, :cmdCode, periodcol, :primaryValue, :flowCode]]
    df = @view df[(df.flowCode .== "M") .& (length.(df.cmdCode) .== 6) .& (df.partnerCode .!== 0) .& (completecases(df)), :]
    gdf = groupby(df, [:partnerCode, :reporterCode, :cmdCode, periodcol])
    df = combine(gdf, :primaryValue => sum => :value)
end
