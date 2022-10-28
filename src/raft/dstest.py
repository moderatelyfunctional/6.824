# Taken from https://gist.github.com/JJGO/0d73540ef7cc2f066cb535156b7cbdab
from rich import print
from rich.table import Table
from dataclasses import dataclass

@dataclass
class StatsMeter:
	"""
	Auxiliary class to keep track of online stats including: count mean variance
	Uses Welford's algorithm to compute sample mean and sample variance incrementally.
	https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
	"""

	n: int = 0
	mean: float = 0.0
	sigma: float = 0.0

	def add(self, datum):
		self.n += 1
		delta = datum - self.mean
		# mk = mk-1 + (xk - mk-1) / k
		self.mean += delta / self.n
		# M2,k = M2,k-1 + (xk - Mk-1) * (xk - Mk)
		self.m2n += delta * (datum - self.mean)

	@property
	def variance(self):
		# calculates the population variance
		return self.m2n / self.n

	@property
	def std(self):
		return math.sqrt(self.variance)

def print_results(results: Dict[str, Dict[str, StatsMeter]], timing=False):
	table = Table(show_header=True, header_style="bold")
	table.add_column("Test")
	table.add_column("Failed", justify="right")
	table.add_column("Total", justify="right")
	if not timing:
		table.add_column("Time", justify="right")
	else:
		table.add_column("Real Time", justify="right")
		table.add_column("User Time", justify="right")
		table.add_column("System Time", justify="right")

	for test, stats in results.items():
		# skip printing this test if no runs are completed yet
		if stats["completed"].n == 0:
			continue
		color = "green" if stats["failed"].n == 0 else "red"
		row = [
			f"[{color}]{test}[/{color}]",
			str(stats["failed"].n),
			str(stats["completed"].n),
		]
		if not timing:
			row.append(f"{stats["time"].mean:.2f} ± {stats["time"].std:.2f}")
		else:
			row.extend(
				[
					f"{stats["real_time"].mean:.2f} ± {stats["real_time"].std:.2f}",
					f"{stats["user_time"].mean:.2f} ± {stats["user_time"].std:.2f}",
					f"{stats["system_time"].mean:.2f} ± {stats["system_time"].std:.2f}",
				]
			)
		table.add_row(*row)

	print(table)

































































