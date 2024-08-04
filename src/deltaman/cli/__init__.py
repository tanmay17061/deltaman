import argparse
from deltaman.samplecollection import JSONSampleCollection
from deltaman.sample import JSONSample
import json

def main():
    # Create the top-level parser
    parser = argparse.ArgumentParser(description="A python library for batch quantitative analysis of JSON payloads (and their deltas).")
    
    # Create subparsers for the "collections" and "samples" commands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create the parser for the "collections_diff" command
    parser_collections = subparsers.add_parser("collections_diff", help="Find the diff between two collections")
    parser_collections.add_argument("collection1", type=str, help="Path to the first collection's directory")
    parser_collections.add_argument("collection2", type=str, help="Path to the second collection's directory")
    parser_collections.add_argument('--max_depth', type=int, default=10, help='Maximum depth of JSON values to explore (default = 10)')

    # Create the parser for the "collection_aggregate" command
    parser_collections = subparsers.add_parser("collection_aggregate", help="Find the aggregate metrics of a collection")
    parser_collections.add_argument("collection", type=str, help="Path to the collection's directory")
    parser_collections.add_argument('--max_depth', type=int, default=10, help='Maximum depth of JSON values to explore (default = 10)')

    # Create the parser for the "samples" command
    parser_samples = subparsers.add_parser("sample", help="Parse a single sample")
    parser_samples.add_argument("sample", type=str, help="Path to the first sample file")
    parser_samples.add_argument('--max_depth', type=int, default=10, help='Maximum depth of JSON values to explore (default = 10)')

    # Parse the arguments
    args = parser.parse_args()
    
    # Implement command functionality
    if args.command == "collections_diff":
        print(collections_diff(args.collection1, args.collection2, args.max_depth))
    elif args.command == "collection_aggregate":
        print(collection_aggregate(args.collection, args.max_depth))
    elif args.command == "sample":
        print(process_samples(args.sample, args.max_depth))
    else:
        parser.print_help()

def collections_diff(collection1, collection2, max_depth):

    sc1 = JSONSampleCollection.from_directory(collection1, max_depth=max_depth)
    sc2 = JSONSampleCollection.from_directory(collection2, max_depth=max_depth)
    return json.dumps(sc1.diff(sc2))

def collection_aggregate(collection, max_depth):

    sc = JSONSampleCollection.from_directory(collection, max_depth=max_depth)
    return json.dumps(sc.get_path_aggregate_scalar_metrics())

def process_samples(sample_path, max_depth):
    sample = JSONSample.from_directory(sample_path, max_depth)
    d = json.dumps(sample.to_dict())
    return d
