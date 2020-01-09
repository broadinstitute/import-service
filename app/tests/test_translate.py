

def test_stream_translate(tmp_path):
    # makes a big file of numbers
    with open(tmp_path / "numbers.txt", 'w') as write_numbers:
        for n in range(10000000):
            write_numbers.write(f"{n}\n")

    # TODO: open the file and pass it to _stream_translate with a fake translator that does nothing
    # and an output of /dev/null
    # use https://github.com/mschwager/memunit to test memory usage

