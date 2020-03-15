import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

data = pd.read_csv('./out/test_out/data_out_bak.csv')

plt.plot(data['n'], data['local'], label="numpy")
plt.plot(data['n'], data['spark'], label="spark")
plt.title("Evolution of the running time in function of the number of edges")
plt.xlabel("Number of edges")
plt.ylabel("Run time (s)")
plt.legend()
plt.show()
