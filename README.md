# Microsimulation with Entity Component System

This code implements a simple microsimulation using the C# Arch Entity Component System framework (https://github.com/genaray/Arch).

The microsimulation models 20 years of births and deaths within a given population.

The model is a CLI application with three command line parametrs:

  --population "path to synthetic population file in protobuf format from UATK-CPS."

  --assumptions "path to a Parquet file containing assumptions."

  --output "path to parquet file were results will be stored."

The synthetic population can be downloaded from (the file will need unpacking to get the protobuf file):
https://alan-turing-institute.github.io/uatk-spc/using_england_outputs.html

The assumptions are included in the Data folder.
