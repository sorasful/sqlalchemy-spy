from sqlalchemy_spy import ConsoleRenderer


class TestConsoleRendererHeader:
    def test_prints_query_count_plural(self, profiler_with_queries, capsys):
        ConsoleRenderer().render(profiler_with_queries)
        assert "2 queries" in capsys.readouterr().out

    def test_prints_query_count_singular(self, profiler_single_query, capsys):
        ConsoleRenderer().render(profiler_single_query)
        assert "1 query" in capsys.readouterr().out

    def test_prints_total_time_in_header(self, profiler_with_queries, capsys):
        ConsoleRenderer().render(profiler_with_queries)
        assert "total" in capsys.readouterr().out

    def test_empty_prints_no_queries_message(self, empty_profiler, capsys):
        ConsoleRenderer().render(empty_profiler)
        assert "no queries recorded" in capsys.readouterr().out


class TestConsoleRendererQueryTable:
    def test_prints_operation(self, profiler_with_queries, capsys):
        ConsoleRenderer().render(profiler_with_queries)
        assert "SELECT" in capsys.readouterr().out

    def test_prints_total_line(self, profiler_with_queries, capsys):
        ConsoleRenderer().render(profiler_with_queries)
        assert "Total:" in capsys.readouterr().out

    def test_error_label_shown(self, profiler_with_error, capsys):
        ConsoleRenderer().render(profiler_with_error)
        assert "[ERROR]" in capsys.readouterr().out


class TestConsoleRendererSlowest:
    def test_top_slow_respected(self, profiler_with_queries, capsys):
        ConsoleRenderer(top_slow=1).render(profiler_with_queries)
        assert "Top 1 slowest" in capsys.readouterr().out

    def test_show_stack_true_shows_callsite(self, profiler_with_queries, capsys):
        ConsoleRenderer(show_stack=True).render(profiler_with_queries)
        assert "↳" in capsys.readouterr().out

    def test_show_stack_false_hides_callsite(self, profiler_with_queries, capsys):
        ConsoleRenderer(show_stack=False).render(profiler_with_queries)
        assert "↳" not in capsys.readouterr().out

    def test_top_slow_zero_skips_section(self, profiler_with_queries, capsys):
        ConsoleRenderer(top_slow=0).render(profiler_with_queries)
        assert "slowest" not in capsys.readouterr().out


class TestConsoleRendererHotPaths:
    def test_hot_paths_shown_by_default(self, profiler_with_queries, capsys):
        ConsoleRenderer().render(profiler_with_queries)
        assert "Hot paths" in capsys.readouterr().out

    def test_show_callsites_false_hides_hot_paths(self, profiler_with_queries, capsys):
        ConsoleRenderer(show_callsites=False).render(profiler_with_queries)
        assert "Hot paths" not in capsys.readouterr().out

    def test_no_stack_skips_hot_paths(self, profiler_no_stack, capsys):
        ConsoleRenderer().render(profiler_no_stack)
        assert "Hot paths" not in capsys.readouterr().out
