import zipline as zp

from ingest import ingest_fmp

# register bundle
zp.data.bundles.register(
    name="name",
    f=ingest_fmp,
    calendar_name="NYSE")

# ingest bundle
zp.data.bundles.ingest("name")

# run algorithm with bundle
zp.run_algorithm(bundle="name")
