import argparse
import latencyconverter.utilities.csv_to_hdf5
import latencyconverter.utilities.json_to_hdf5


def main():
    # Initialize argument parser
    argsparser = argparse.ArgumentParser()

    # Add argument for specifying csv file
    argsparser.add_argument(
        '-c',
        '--csv',
        help='Specify path to source csv file',
        default=None
    )

    # Add argument for specifying json file
    argsparser.add_argument(
        '-j',
        '--json',
        help='Specify path to source json file',
        default=None
    )

    # Add argument for specifying destination file
    argsparser.add_argument(
        '-d',
        '--destination',
        help='Specify path to destination hdf5 file.',
        required=True
    )

    # Parse the args!
    args = argsparser.parse_args()

    # Fail if the user tried to specify two different source files
    if args.csv is not None and args.json is not None:
        raise ValueError("Can't specify csv file AND json file. Pick one!")
    elif args.csv is not None:
        csvDF = latencyconverter.utilities.csv_to_hdf5.load_csv(args.csv)

        latencyconverter.utilities.csv_to_hdf5.csv_to_h5py(args.destination,
                                                           csvDF)
    elif args.json is not None:
        availability = latencyconverter.utilities.json_to_hdf5.load_json(
            '../sampledata/QW.QCC01.2022.044.json')

        latency_df = latencyconverter.utilities.json_to_hdf5.json_to_table(
            availability)

        latencyconverter.utilities.json_to_hdf5.json_to_hdf5(
            '../sampledata/QW.QCC01.2022.044.hdf5', latency_df)

    else:
        raise ValueError("No source file specified!")


if __name__ == '__main__':
    main()
