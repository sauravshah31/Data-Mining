import numpy as np

data = np.array([
    [2,10],
    [2,5],
    [8,4],
    [5,8],
    [7,5],
    [6,4],
    [1,2],
    [4,9]
])

UNDEFINED = -2
NOISE = -1

data_point_names = ["A1","A2","A3","B1","B2","B3","C1","C2"]
labels = [UNDEFINED for _ in range(data.shape[0])]

def eucledian_distance(x,y):
    return np.sqrt(np.sum((x-y)**2))


def get_epsilon_neignbours(point,epsilon):
    global data
    neighbours = []
    for i in range(data.shape[0]):
        curr_distance = eucledian_distance(point,data[i])
        if curr_distance <= epsilon:
            neighbours.append(i)
    return neighbours

def train(epsilon,min_points):
    global data,clusters,labels
    c = 0

    for i in range(len(labels)):
        labels[i] = UNDEFINED

    for i,point in enumerate(data[:]):
        if labels[i]!=UNDEFINED:
            continue
        
        curr_neighbours = get_epsilon_neignbours(point,epsilon)

        if len(curr_neighbours)<min_points:
            labels[i]=NOISE
            continue

        c += 1
        labels[i] = c

        curr_neighbours.remove(i)
        for n in curr_neighbours:
            if labels[n] == NOISE:
                labels[n] = c
            if labels[n] != UNDEFINED:
                continue
            labels[n] = c

            n_neighbour = get_epsilon_neignbours(data[n],epsilon)
            if len(n_neighbour) >= min_points:
                curr_neighbours.extend(n_neighbour)

    return c


def db_scan_algo(epsilon,min_points):
    nclusters = train(epsilon,min_points)
    print(f"No of clusters formed = {nclusters}")
    noise = []
    for c in range(nclusters):
        print(f"Cluster {c+1}:")
        for index in range(data.shape[0]):
            if labels[index] == (c+1):
                print(f"{data_point_names[index]}{data[index]} ",end="")
            elif labels[index] == NOISE:
                noise.append(index)
        print()
        print()
    print("Noise")
    for index in noise:
        print(f"{data_point_names[index]}{data[index]} ",end="")
    print()


if __name__=="__main__":
    print(f"Taking (epsilon,min_points)=({3.5},{2})-----")
    db_scan_algo(3.5,2)
    print("------------------------------------------")
    print(f"Taking (epsilon,min_points)=({3.1},{2})-----")
    db_scan_algo(3.1,2)    
    print()