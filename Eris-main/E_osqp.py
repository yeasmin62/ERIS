# import osqp
# import numpy as np
# import scipy as sp
# from scipy import sparse
import json

# # Define P, q, A, l, u
# P = sparse.csc_matrix([[4, 1], [1, 2]])
# q = np.array([1, 1])
# A = sparse.csc_matrix([[1, 1], [1, 0], [0, 1]])
# l = np.array([1, 0, 0])
# u = np.array([1, 0.7, 0.7])

# # Create an OSQP object
# prob = osqp.OSQP()

# # Setup the problem
# prob.setup(P, q, A, l, u,max_iter = 1)

# # Solve the problem
# res = prob.solve()
# print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))

import osqp
import numpy as np
import scipy as sp
from scipy import sparse
def get(x, i): 
    return np.array([r[i] for r in x])

data = np.array([(1.0,0,0),(1.0,1,1),(-1.0,1,2),(-1.0,1,3),(-1.0,1,4),(-1.0,1,5),(-1.0,1,6),(-1466.0299682617188,1,0),(-1.0,2,7),(-1.0,2,8),(-1.0,2,9),(-1.0,2,10),(-1.0,2,11),(1.0,2,12),(-1.0,2,13),(-1.0,2,14),(-1.0,2,15),(-4400.369934082031,2,0),(-1.0,2,16),(-1.0,2,17),(-1.0,2,18),(-1.0,2,19),(-1.0,2,20),(-1.0,2,21),(-1.0,2,22),(1.0,3,1),(-1.0,3,23),(-1.0,3,24),(-1.0,3,25),(-1.0,3,26),(-1.0,3,27),(-1.0,3,28),(-1.0,3,29),(-1.0,3,30),(-1.0,3,31),(-1.0,3,32),(-1.0,3,33),(-1.0,3,34),(-1.0,3,35),(-1.0,3,36),(-1.0,3,37),(-4402.829864501953,3,0),(-293.5099792480469,4,0),(1.0,4,38),(-1.0,4,39),(-1.0,5,40),(-1.0,5,41),(-1.0,5,42),(-1.0,5,43),(-4402.679901123047,5,0),(-1.0,5,44),(-1.0,5,45),(-1.0,5,46),(-1.0,5,47),(-1.0,5,48),(-1.0,5,49),(1.0,5,38),(-1.0,5,50),(-1.0,5,51),(-1.0,5,52),(-1.0,5,53),(-1.0,5,54),(-1.0,6,55),(-1.0,6,56),(-1.0,6,57),(1.0,6,58),(-1466.6499633789062,6,0),(-1.0,6,59),(-1.0,6,60),(-1.0,7,61),(-1.0,7,62),(-1.0,7,63),(-1.0,7,64),(-1.0,7,65),(-1.0,7,66),(-1.0,7,67),(-1.0,7,68),(1.0,7,58),(-1.0,7,69),(-4401.869842529297,7,0),(-1.0,7,70),(-1.0,7,71),(-1.0,7,72),(-1.0,7,73),(-1.0,7,74),(-1.0,7,75),(1.0,8,76),(-1.0,8,77),(-1.0,8,78),(-1.0,8,79),(-1466.18994140625,8,0),(-1.0,8,80),(-1.0,8,81),(1.0,9,76),(-1.0,9,82),(-1.0,9,83),(-1.0,9,84),(-1.0,9,85),(-1.0,9,86),(-1.0,9,87),(-1.0,9,88),(-1.0,9,89),(-1.0,9,90),(-1.0,9,91),(-4400.999908447266,9,0),(-1.0,9,92),(-1.0,9,93),(-1.0,9,94),(-1.0,9,95),(-1.0,9,96),(-1.0,10,97),(-1.0,10,98),(-1.0,10,99),(1.0,10,12),(-1.0,10,100),(-1.0,10,101),(-1466.4899291992188,10,0),])
# Generate problem data
sp.random.seed(1)
m = 11
n = 102
Ad = sparse.csc_matrix(sparse.coo_matrix((get(data,0),(get(data,1),get(data,2))),shape=(11,102)))
print(Ad.toarray())
b = np.random.randn(m)

# OSQP data
P = sparse.block_diag([sparse.csc_matrix((n, n)), sparse.eye(m)], format='csc')
q = np.zeros(n+m)
A = sparse.bmat([[Ad,            -sparse.eye(m)],
                 [sparse.eye(n),  None]], format='csc')
print(A.toarray())
l = np.hstack([np.ones(1),np.zeros(m-1), -np.inf*np.ones(n)])
u = np.hstack([np.ones(1),np.zeros(m-1), np.inf*np.ones(n)])

# Create an OSQP object
prob = osqp.OSQP()

# Setup workspace
prob.setup(P, q, A, l, u)

# Solve problem
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))