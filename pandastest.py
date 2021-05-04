import numpy as np
import pandas as pd

df = pd.DataFrame(np.random.randn(10, 4))
pieces = [df[:3], df[1:2], df[1:4]]
pd.concat(pieces)
print(pieces)