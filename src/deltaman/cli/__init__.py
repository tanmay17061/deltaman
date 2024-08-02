import argparse
from deltaman.samplecollection import JSONSampleCollection
def main():
    # Create the top-level parser
    parser = argparse.ArgumentParser(description="A python library for batch quantitative analysis of JSON payloads (and their deltas).")
    
    # Create subparsers for the "collections" and "samples" commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create the parser for the "collections" command
    parser_collections = subparsers.add_parser("collections", help="Process collections")
    parser_collections.add_argument("collection1", type=str, help="Path to the first collection's directory")
    parser_collections.add_argument("collection2", type=str, help="Path to the second collection's directory")

    # Create the parser for the "samples" command
    parser_samples = subparsers.add_parser("samples", help="Process samples")
    parser_samples.add_argument("sample1", type=str, help="Path to the first sample file")
    parser_samples.add_argument("sample2", type=str, help="Path to the second sample file")
    
    # Parse the arguments
    args = parser.parse_args()
    
    # Implement command functionality
    if args.command == "collections":
        return process_collections(args.collection1, args.collection2)
    elif args.command == "samples":
        process_samples(args.sample1, args.sample2)
    else:
        parser.print_help()

def process_collections(collection1, collection2):

    sc1 = JSONSampleCollection.from_directory(collection1)
    sc2 = JSONSampleCollection.from_directory(collection2)
    return sc1.diff(sc2)

def process_samples(sample1, sample2):
    raise NotImplementedError("Yet to parse JSONSample from file path.")
