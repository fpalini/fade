# FADE

*FADE (Fast Alignment-free Distributed Environment)* is an extensible framework, developed to efficiently compute alignment-free (AF) functions on a set of large genomic sequences. It runs over *Apache Spark* (>=2.3, https://spark.apache.org/) and requires a *Java* compliant virtual machine (>= 1.8, https://adoptopenjdk.net). Moreover, it can perform Monte Carlo hypothesis test simulations, in order to analyze the AF matrix computed on the set of sequences.

**NOTE**: an extended version of this documentation is included in *MANUAL.pdf*.

### The processing pipeline

![FADE Pipeline](https://drive.google.com/uc?export=view&id=1VlMbtyTNIhGph6m5-HBcajTRfUsSXfjO)


## Usage
The software is released as a single executable jar file, **fade-1.0.0-all.jar**, that can be used to run FADE from the command line, together with the *Apache Spark*  `spark-submit` command, using the following syntax:

`spark-submit fade-1.0.0-all.jar [conf-file]`

If `conf-file` is not specified, the program will look for a `fade.conf` file in the working directory. 
FADE has two main operational tasks:
- *distance evaluation*: a set of AF matrices is evaluated starting from a collection of input genomic sequences and according to a provided list of AF functions;
- *montecarlo simulations*: run a statistical significance test on a set of input AF functions with respect to a given collection of genomic sequences

## Quickstart
For a quick start, assuming both Apache Spark and Java are properly installed, move in the package root directory and run the following command:

`spark-submit fade-1.0.0-all.jar quickstart.conf`

As a result, FADE will be used to evaluate the k-mer based Euclidean AF function, with k=5, on the Mitocondria dataset (included in this package). The resulting distance matrix will be saved in the *output* directory. The settings of this experiment are defined in the *quickstart.conf* configuration file.

## Configuration File

The creation of a proper configuration file is the recommended way to instruct *FADE* about the experiments to perform, as it does not require any programming skill. Alternatively, it is possible to write a Java application using he FADE facilities in thanks to the provided programming API (see the `Main.java` class for an example).

### Example 1 - Evaluating Kmer-based Statistics

In this example, the k-mer based D2 and Euclidean AF functions are evaluated on a collection of FASTA files stored in the data/mito directory (see [Available K-mer based AF functions](#available-k-mer-based-af-functions)  for a complete list of the supported functions). The input directory and the output name are set using, respectively, the `input` and `output` parameters. The parameter `k` defines the lenght of the k-mers to extract. The AF functions to evaluate are specified using the `evaluator` parameter. The `local` parameter has to be `false` if *FADE* runs on a computer cluster; `true`, if it is run on a stand-alone machine.

If the dataset consists of unassembled sequences, i.e. each file contains one or more reads, the `assembled` parameter has to be set with `no` (see Example 3).

```
task=distance

k=5

local=false

input=data/mito
output=dist_mito

extractor=fade.kmer.fast.FastKmerExtractorByBin
aggregator=fade.kmer.fast.FastKmerAggregatorByBin
evaluator=fade.affunction.D2, fade.affunction.Euclidean
```

### Example 2 - Evaluating Kmer-based Statistics with MinHash approximation

In this example, the Mash AF function is evaluated on a collection of FASTA files stored in the data/mito directory. The input directory and the output name are set using, respectively, the `input` and `output` parameters. The parameter `k` defines the lenght of the k-mers to extract. The parameter `s` defines the size of the sketch to compute for each sequence. The AF function to evaluate is specified using the `evaluator` parameter. The `local` parameter has to be `false` if *FADE* runs on a computer cluster; `true`, if it is run on a stand-alone machine.

If the dataset consists of unassembled sequences, i.e. each file contains one or more reads, the `assembled` parameter has to be set with `no` (see Example 3).

```
task=distance

k=5
s=1000

local=false

input=data/mito
output=dist_mito

extractor=fade.mash.MashExtractor
aggregator=fade.mash.MashAggregator
evaluator=fade.affunction.Mash
```

### Example 3 - Evaluating Spaced word-based Statistics

In this example, the spaced-word based FSWM AF function is evaluated on a collection of FASTA files stored in the data/mito directory (see [Available Word Matches based AF functions](#available-word-matches-based-af-functions)  for a complete list of the supported functions). The input directory and the output name are set using, respectively, the `input` and `output` parameters. The  `pattern` parameter defines the pattern of `care` and `don't care` characters to use for the spaced words extraction. The AF function to evaluate is set using the `evaluator` parameter. The `local` parameter has to be `false` if *FADE* runs on a computer cluster; `true`, if it is run on a stand-alone machine.

If the dataset consists of unassembled sequences, i.e. each file contains one or more reads, the `assembled` parameter has to be set with `no` (see Example 3).

```
task=distance

k=20

pattern=10001011010100101001
threshold=0

local=false

input=data/mito
output=dist_mito

extractor=fade.sw.SwExtractorByBin
aggregator=fade.sw.SwAggregatorByBin
evaluator=fade.affunction.FSWM
```

### Example 4 - Running Monte Carlo Simulation

In this example, a *Monte Carlo Simulation* is performed to test the statistical significance of the Canberra and of the Jaccad AF functions. The `task` parameter is set to `simulation`. The `simulations` parameter sets the number of simulations to perform. The `q` parameter is used by the procedure to generate the synthetic datasets. For the other parameters, we refer the reader to Example 1 and Example 2. More information about the algorithm use to generate synthetic datasets and to run Monte Carlo simulations are available in the extended document included in the FADE software package and in the following paper: [Alignment-free Genomic Analysis via a Big Data Spark Platform](https://arxiv.org/abs/2005.00942).




```
task=simulation

k=12

simulations=5
q=10

assembled=no
local=true

input=data/ecoli_0125
output=sim_ecoli_0125_q10_n10

extractor=fade.kmer.KmerExtractorByBin
aggregator=fade.kmer.KmerAggregatorByBin
evaluator=fade.affunction.Canberra, fade.affunction.Jaccard
```


## Supported Statistics and AF functions

Actually, two types of statistics, described as follows, are natively supported by the framework.

### K-mer Statistics

For each sequence in the set, the contiguous subwords of length *k* therein contained (i.e., *k-mers*) with their associated frequencies are counted. The result is a set of vectors. Then, sequences are compared pairwise by computing suitable AF functions, between each pair of vectors.

#### Available K-mer based AF functions

For each AF function, it is reported the full name of the Java class to be used for recalling that function in *FADE*.

- Canberra Distance (`fade.affunction.Canberra`)
- Chebyshev Distance (`fade.affunction.Chebyshev`)
- ![ChiSquare](https://render.githubusercontent.com/render/math?math=\chi^2) Distance (`fade.affunction.ChiSquare`)
- ![D2](https://render.githubusercontent.com/render/math?math=D_2) Similarity (`fade.affunction.D2`)
- ![D2S](https://render.githubusercontent.com/render/math?math=D_2^S) Similarity (`fade.affunction.D2S`)
- ![D2Z](https://render.githubusercontent.com/render/math?math=D_2^Z) Similarity (`fade.affunction.D2Z`)
- ![D2*](https://render.githubusercontent.com/render/math?math=D_2^*) Similarity (`fade.affunction.D2star`)
- Euclidean Distance (`fade.affunction.Euclidean`)
- Harmonic Mean Similarity (`fade.affunction.HarmonicMean`)
- Intersection Similarity (`fade.affunction.Intersection`)
- Jaccard Similarity (`fade.affunction.Jaccard`)
- Jeffrey's Divergence (`fade.affunction.Jeffrey`)
- Jensen-Shannon Divergence (`fade.affunction.JensenShannon`)
- Kulczynski2 Similarity (`fade.affunction.Kulczynski2`)
- Manhattan Distance (`fade.affunction.Manhattan`)
- Mash Distance (`fade.affunction.Mash`)
- Squared-Chord Distance (`fade.affunction.SquaredChord`)



### Word Matches Statistics

Given a binary pattern *P* of match (1) and don’t care (0) characters, there exists a *spaced word matching* between two sequences *s*, *t*, respectively in positions *i1* and *i2*, according to the pattern *P* with length *l*, if for each match position *m* in *P*, it is true that s[i1 + m] = t[i2 + m].

#### Available Word Matches based AF functions
- FSWM Distance (`fade.affunction.FSWM`)

## Datasets

FADE has been extensively tested by using the following genomic datasets:
- [E.coli/Shigella (39MB)](http://afproject.org/media/genome/hgt/unsimulated/ecoli_shigella/dataset/unsimulated-ecoli_shigella.zip)
- [Mitocondria (0.2MB)](http://afproject.org/media/genome/std/assembled/fish_mito/dataset/assembled-fish_mito.zip)
- [Plants (1.3GB)](http://afproject.org/media/genome/std/assembled/plants/dataset/assembled-plants.zip)
- [Unassembled E.coli (coverage 0.03125, 0.125, 1) (309MB)](http://afproject.org/media/genome/std/unassembled/ecoli/dataset/unassembled-ecoli.zip)
- [Unassembled Plants (coverage 1) (2.7GB)](http://afproject.org/media/genome/std/unassembled/plants/dataset/unassembled-plants.zip)
- [Yersinia (11MB)](http://afproject.org/media/genome/hgt/unsimulated/yersinia/dataset/unsimulated-yersinia.zip)


## Acknowledgments 
This software was developed to support the experimental part of research partially supported by  the PRIN project  
Multicriteria Data Structures and Algorithms: from compressed to learned indexes, and beyond
PRIN no. 2017WR7SHH
