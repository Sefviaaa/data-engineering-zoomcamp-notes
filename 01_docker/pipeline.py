import sys
import pandas as pd

print(sys.argv) # command line arguments that we pass to script

day = sys.argv[1]

# some fancy stuff with pandas: loading csv file, 
print(f'job finished successfully for day = {day}')