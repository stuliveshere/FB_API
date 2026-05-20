# nbconvert execution settings for the DATA7201 edge node.
#
# The YARN cluster is heavily shared, so a Spark kernel can take several
# minutes to be allocated. nbconvert's default 60s startup timeout is far
# too short, and cell execution can run long while waiting on executors.
#
# Install once so every `jupyter nbconvert` run picks it up automatically:
#
#   mkdir -p ~/.jupyter
#   cp scripts/jupyter_nbconvert_config.py ~/.jupyter/
#
# After that, plain `jupyter nbconvert --to notebook --execute <nb>` works
# with no extra flags.

c = get_config()  # noqa: F821

# Seconds to wait for the kernel to come up before giving up.
c.ExecutePreprocessor.startup_timeout = 3600

# Seconds to wait for any single cell to finish.
c.ExecutePreprocessor.timeout = 3600
