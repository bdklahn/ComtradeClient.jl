"Comtrade API host url"
const domain = "comtradeapi.un.org"

"API Version"
const v = "v1"

"API types"
@enum API bulk

"Type of trade: C for commodities and S for service"
@enum typeCode C S

"Trade frequency: A for annual and M for monthly"
@enum freqCode A M

"Trade (IMTS) classifications: HS, SITC, BEC or EBOPS."
@enum clCode HS H0 H1 H2 H3 H4 H5 H6 SITC BEC EBOPS

"Columns which can be encoded with much fewer posibilities than the full alphabet of strings."
const categoricals = Set((
    :datasetCode,
    :typeCode,
    :freqCode,
    :flowCode,
    :classificationSearchCode,
    :classificationCode,
    :cmdCode,
    :customsCode,
    :partnerCode,
    :partner2Code,
    :reporterCode,
    :mosCode,
    :motCode,
    :qtyUnitCode,
    :altQtyUnitCode,
    ))




"UN Classifications on economic statistics (https://unstats.un.org/unsd/classifications/Econ) HS file base url"
const HS_json_url = "https://comtradeapi.un.org/files/v1/app/reference/"

"The base url where reference files are"
const file_app_ref_url = "https://comtradeapi.un.org/files/v1/app/reference/"

"Reference of reporting entities (E.g. to look up information about reporterCode)"
const reporters_url = file_app_ref_url*"Reporters.json"

"Reference of parter areas (E.g. to look up information about parterCode)"
const partner_areas_url = file_app_ref_url*"partnerAreas.json"

"Reference of The unit for quantity and alternate quantity data items (E.g. to look up information about qtyUnitCode)"
const qty_units_url = file_app_ref_url*"QuantityUnits.json"

"data columns we should make sure are encoded as Bool (vs., say, Float64)"
const bool_cols = Set((
    :isAggregate,
    :isAltQtyEstimated,
    :isGrossWgtEstimated,
    :isNetWgtEstimated,
    :isOriginalClassification,
    :isQtyEstimated,
    :isReported,
    ))

"data columns we should make sure are encoded as Int"
const int_cols = Set((
    :datasetCode,
    :refPeriodId,
    :refYear,
    :refMonth,
    :period,
    :reporterCode,
    :partnerCode,
    :partner2Code,
    :mosCode,
    :motCode,
    :qtyUnitCode,
    :altQtyUnitCode,
    :legacyEstimationFlag,
    ))

"data columns we should make sure are encoded as Float"
const float_cols = Set((
    :qty,
    :altQty,
    :netWgt,
    :grossWgt,
    :CIFValue,
    :FOBValue,
    :primaryValue,
    ))

const projectdir = get(ENV, "PROJECT_DIR", "/project/bi_dpi")

"Set the default datadir according to whether the project directory exists."
const datadir = joinpath(isdir(projectdir) ? projectdir : "./", "data/UN_Comtrade")

"Gallium Nitride (GaN) & Indium Phosphide (InP) HS codes"
const GaN_InP_HS_codes = Set(("285000", "811291", "811299"))
