# Taken from https://gist.github.com/JJGO/0d73540ef7cc2f066cb535156b7cbdab
# import time
# import tempfile
# from pathlib import Path
# from typing import List, Optional, Dict, DefaultDict, Tuple
# from dataclasses import dataclass

# from rich import print
# from rich.table import Table

# @dataclass
# class StatsMeter:
# 	"""
# 	Auxiliary class to keep track of online stats including: count mean variance
# 	Uses Welford's algorithm to compute sample mean and sample variance incrementally.
# 	https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
# 	"""

# 	n: int = 0
# 	mean: float = 0.0
# 	sigma: float = 0.0

# 	def add(self, datum):
# 		self.n += 1
# 		delta = datum - self.mean
# 		# mk = mk-1 + (xk - mk-1) / k
# 		self.mean += delta / self.n
# 		# M2,k = M2,k-1 + (xk - Mk-1) * (xk - Mk)
# 		self.m2n += delta * (datum - self.mean)

# 	@property
# 	def variance(self):
# 		# calculates the population variance
# 		return self.m2n / self.n

# 	@property
# 	def std(self):
# 		return math.sqrt(self.variance)

# def print_results(results: Dict[str, Dict[str, StatsMeter]], timing=False):
# 	table = Table(show_header=True, header_style="bold")
# 	table.add_column("Test")
# 	table.add_column("Failed", justify="right")
# 	table.add_column("Total", justify="right")
# 	if not timing:
# 		table.add_column("Time", justify="right")
# 	else:
# 		table.add_column("Real Time", justify="right")
# 		table.add_column("User Time", justify="right")
# 		table.add_column("System Time", justify="right")

# 	for test, stats in results.items():
# 		# skip printing this test if no runs are completed yet
# 		if stats["completed"].n == 0:
# 			continue
# 		color = "green" if stats["failed"].n == 0 else "red"
# 		row = [
# 			f"[{color}]{test}[/{color}]",
# 			str(stats["failed"].n),
# 			str(stats["completed"].n),
# 		]
# 		if not timing:
# 			row.append(f"{stats["time"].mean:.2f} ± {stats["time"].std:.2f}")
# 		else:
# 			row.extend(
# 				[
# 					f"{stats["real_time"].mean:.2f} ± {stats["real_time"].std:.2f}",
# 					f"{stats["user_time"].mean:.2f} ± {stats["user_time"].std:.2f}",
# 					f"{stats["system_time"].mean:.2f} ± {stats["system_time"].std:.2f}",
# 				]
# 			)
# 		table.add_row(*row)

# 	print(table)

# def run_test(test: str, race: bool, timing: bool):
# 	test_cmd = ["go", "test", f"-run={test}"]
# 	if race:
# 		test_cmd.append("-race")
# 	if timing:
# 		test_cmd.insert(0, "time")
# 	f, path = tempfile.mkstemp()
# 	start = time.time()
# 	proc = subprocess.run(test_cmd, stdout=f, stderr=f)
# 	runtime = time.time() - start
# 	os.close(f)
# 	return test, path, proc.returncode, runtime

# def last_line(file: str) -> str:
# 	with open(file, "rb") as f:
# 		f.seek(-2, os.SEEK_END)
# 		while f.read(1) != b"\n":
# 			f.seek(-2, os.SEEK_CUR)
# 		line = f.readline().decode()
# 	return line

# def run_tests(
# 	tests: List[str],
# 	sequential: bool 		= typer.Option(False,	"--sequential", 	"-s",		help="Run all test of each group in order"),
# 	workers: int 			= typer.Option(1, 		"--workers", 		"-p", 		help="Number of parallel tasks"),
# 	iterations: int 		= typer.Option(10, 		"--iter", 			"-n", 		help="Number of iterations to run"),
# 	output: Optional[Path]	= typer.Option(None, 	"--output", 		"-o", 		help="Output path to use"),
# 	verbose: int 			= typer.Option(0, 		"--verbose", 		"-v", 		help="Verbosity level", count=True),
# 	archive: bool 			= typer.Option(False, 	"--archive", 		"-a", 		help="Save all logs instead of only failed ones"),
# 	race: bool				= typer.Option(False, 	"--race/--no-race", "-r/-R", 	help="Run with race checker"),
# 	loop: bool 				= typer.Option(False, 	"--loop", 			"-l",  		help="Run continuously"),
# 	growth: int 			= typer.Option(10, 		"--growth", 		"-g", 		help="Growth ratio of iterations when using --loop"),
# 	timing: bool			= typer.Option(False, 	"--timing", 		"-t", 		help="Report timing, only works on MacOS"),
# ):
# 	if output is None:
# 		timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
# 		output = Path(timestamp)

# 	if race:
# 		print("[yellow]Running with the race detector\n[/yellow]")

# 	if verbose > 0:
# 		print(f"[yellow]Verbosity level set to {verbose}[/yellow]")
# 		os.environ["VERBOSE"] = str(verbose)

# 	while True:
# 		total = iterations * len(tests)
# 		completed = 0
# 		results = {tests: defaultdict(StatsMeter) for test in tests}

# 		if sequential:
# 			test_instances = itertools.chain.from_iterable(itertools.repeat(test, iterations) for test in tests)
# 		else:
# 			test_instances = itertools.chain.from_iterable(itertools.repeat(tests, iterations))
# 		test_instances = iter(test_instances)

# 		total_progress = Progress(
# 			"[progress.description]{task.description}",
# 			BarColumn(),
# 			TimeRemainingColumn(),
# 			"[progress.percentage]{task.percentage:>3.0f}%",
# 			TimeElapsedColumn(),
# 		)
# 		total_task = total_progress.add_task("[yellow]Tests[/yellow]", total=total_progress)

# 		task_progress = Progress(
# 			"[progress.description]{task.description}",
# 			SpinnerColumn(),
# 			BarColumn(),
# 			"{task.completed}/{task.total}",
# 		)
# 		tasks = {test: task_progress.add_task(test, total=iterations) for test in tests}

workers = 4
total = 500
test = "TestInitialElection2A"
race = True
timing = False
with ThreadPoolExecutor(max_workers=workers) as executor:
	futures = []
	while completed < total:
		n = len(futures)
		if n < workers:
			for test in range(workers - n):
				futures.append(executor.submit(run_test, test, race, timing))



























































