# go-chunker

## Overview

The chunker with Go is designed to split large input files or directories into smaller chunks based on the number of rows. This is particularly useful for splitting up larger data sets into smaller chunks.

## Installation

To install the Chunk Processor, you need to have Go installed on your system. You can download and install Go from the official [Go website](https://golang.org/dl/).

Once Go is installed, you can install the chunker using the following command:

```sh
go install cmd/chunk/main.go
```
or 
```sh
go install github.com/johannessarpola/go-chunker/cmd/chunk@latest
```

## Usage

You can run the Chunk Processor using the following commands:

### For Directory Input

```sh
chunk -o out -i .in/dir -s 10000
```

### For File Input

```sh
chunk -o out -i .in/products_chunk.csv -s 10000
```

### Command-Line Options

- `-o` or `--output`: Specifies the directory where the output chunks will be saved.
- `-i` or `--input`: Specifies the input file or directory to be processed.
- `-s` or `--size`: Specifies the number of rows for each chunk.
- `-h` or `--help`: Help for the CLI

### Example

To split a directory `.in/dir` into chunks of 10,000 rows each and save the output to the `out` directory:

```sh
chunk -o out -i .in/dir -s 10000
```

To split a file `.in/products_chunk.csv` into chunks of 10,000 rows each and save the output to the `out` directory:

```sh
chunk -o out -i .in/products_chunk.csv -s 10000
```

## Detailed Description

### Input Handling

The tool supports two types of input:

1. **Directory Input**: When the input is a directory, the tool processes all files within the directory.
2. **File Input**: When the input is a single file, the tool processes that file.

### Output Handling

The output chunks are saved in the specified output directory. Each chunk is named sequentially (e.g., `chunk1`, `chunk2`, etc.). Additionally, for each chunk, a corresponding meta JSON file is generated with the following structure:

```json
{
  "id": 4,
  "file": "<output path>",
  "min": 10,
  "max": 19,
  "alive_duration": "12.34ms",
  "active_duration": "1.23ms"
}
```

- `id`: The sequential ID of the chunk which is also the id for writer.
- `file`: The path to the chunk file.
- `min`: The starting row number for the chunk.
- `max`: The ending row number for the chunk.
- `alive_duration`: The total duration for the corresponding writer.
- `active_duration`: The active processing duration for the chunk.

### Chunking Logic

The tool reads the input file(s) and splits them into chunks based on the specified number of rows. Each chunk is written to a separate file in the output directory, and a corresponding meta JSON file is generated for each chunk.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
