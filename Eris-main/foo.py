
import osqp
import numpy as np
from scipy import sparse
import math
import json

def get(x, i): 
    return np.array([r[i] for r in x])

data = np.array([(1.0,0,0),(1.0,1,1),(293.2799987792969,1,0),(293.2799987792969,1,2),(1.0,2,1),(293.5199890136719,2,0),(146.75999450683594,2,3),(146.75999450683594,2,4),(1.0,3,5),(73.35250091552734,3,6),(73.37249755859375,3,7),(73.3949966430664,3,8),(73.35250091552734,3,9),(293.47249603271484,3,0),(48.92166646321614,4,10),(48.91999816894531,4,11),(48.923329671223954,4,12),(48.92166646321614,4,13),(48.91499837239583,4,14),(1.0,4,5),(48.91999816894531,4,15),(293.5216573079427,4,0),(1.0,5,16),(293.239990234375,5,0),(146.60499572753906,5,17),(146.63499450683594,5,18),(1.0,6,16),(293.42498779296875,6,0),(146.72499084472656,6,19),(146.6999969482422,6,20),(48.906667073567704,7,21),(48.86833190917969,7,22),(48.875,7,23),(48.89333089192708,7,24),(293.2849934895833,7,0),(1.0,7,25),(48.87833150227864,7,26),(48.863332112630204,7,27),(48.906667073567704,8,28),(48.91166687011719,8,29),(48.89666748046875,8,30),(293.4333292643229,8,0),(48.913330078125,8,31),(1.0,8,25),(48.90333048502604,8,32),(48.90166727701823,8,33),])

P = sparse.csc_matrix(sparse.diags([0.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,])
)
q = np.array(np.zeros(34))
A = sparse.csc_matrix(sparse.coo_matrix((get(data,0),(get(data,1),get(data,2))),shape=(9,34)))
l = np.hstack([np.ones(1),np.zeros(9-1)])
u = np.hstack([np.ones(1),np.inf*np.ones(9-1)])

prob = osqp.OSQP()
prob.setup(P, q, A, l, u,  verbose=False)
res = prob.solve()
print(json.dumps({"solution": res.x.tolist(), "objective": res.info.obj_val}))