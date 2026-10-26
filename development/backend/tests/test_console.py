from src.utils.console import stderr_logger, stdout_logger


def test_console_loggers_write_messages_as_given(capsys):
    stdout_logger().info("100% done: %s")  # no args, so % is left alone
    stderr_logger().info("")  # a blank line, like print()
    out = capsys.readouterr()
    assert out.out == "100% done: %s\n"
    assert out.err == "\n"


def test_end_keeps_a_progress_line_open(capsys):
    err = stderr_logger()
    err.info("\rFetched 1 posts...", extra={"end": ""})
    err.info("\rFetched 2 posts...", extra={"end": ""})
    err.info("")
    assert capsys.readouterr().err == "\rFetched 1 posts...\rFetched 2 posts...\n"


def test_console_lines_stay_out_of_the_root_logger(caplog):
    stdout_logger().info("console only")
    assert "console only" not in caplog.text
