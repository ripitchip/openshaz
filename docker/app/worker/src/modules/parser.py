import argparse
import sys


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Audio Feature Extraction Module")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    manual_parser = subparsers.add_parser(
        "manual", help="Run manual audio processing and analysis"
    )
    manual_parser.add_argument(
        "--debug", action="store_true", help="Run with debug console"
    )
    manual_parser.add_argument(
        "--multi", action="store_true", help="Enable multiprocessing"
    )
    manual_parser.add_argument(
        "--limit", action="store_true", help="Limit number of audio files imported"
    )
    manual_parser.add_argument(
        "--recreate", action="store_true", help="Force recreation of dataset cache"
    )
    manual_parser.add_argument(
        "--source",
        type=str,
        default="gtzan",
        choices=["gtzan", "fma"],
        help="Dataset source: 'gtzan' or 'fma' (default: gtzan)",
    )
    manual_parser.add_argument(
        "--fma-size",
        type=str,
        default="small",
        choices=["small", "medium", "large", "full"],
        help="FMA dataset size: 'small' (8GB), 'medium' (25GB), 'large' (93GB), or 'full' (879GB) (default: small)",
    )
    manual_parser.add_argument(
        "--force",
        action="store_true",
        help="Force download of large datasets like fma_full",
    )
    manual_parser.add_argument(
        "--compare-metrics",
        action="store_true",
        help="Compare different similarity metrics",
    )
    manual_parser.add_argument(
        "--metric",
        type=str,
        default="cosine",
        choices=["cosine", "euclidean", "manhattan"],
        help="Similarity metric to use (default: cosine)",
    )
    manual_parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of top similar results (default: 5)",
    )
    manual_parser.add_argument(
        "--test-audio-path",
        type=str,
        default=None,
        help="Path to the test audio file",
    )

    worker_parser = subparsers.add_parser(
        "worker", help="Run as a worker processing tasks from queue"
    )
    worker_parser.add_argument(
        "--debug", action="store_true", help="Run with debug console"
    )
    worker_parser.add_argument(
        "--queue-url",
        type=str,
        help="Queue URL for receiving tasks",
    )
    worker_parser.add_argument(
        "--worker-id",
        type=str,
        help="Worker identifier",
    )
    worker_parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of tasks to process in parallel (default: 10)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    return args
