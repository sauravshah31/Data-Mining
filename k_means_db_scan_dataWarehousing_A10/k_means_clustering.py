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

data_point_names = ["A1","A2","A3","B1","B2","B3","C1","C2"]
labels = [0 for _ in range(data.shape[0])]

def eucledian_distance(x,y):
    return np.sqrt(np.sum((x-y)**2))

def train(k,epochs):
    global labels,data
    #randomly choose k data points
    n_points = data.shape[0]
    index = [i for i in range(n_points)]
    np.random.shuffle(index)
    k_index = index[:k]
    k_points = np.array([data[i] for i in k_index])

    #train the model
    for _ in range(epochs):
        #assign the points to the nearest point
        #we use eucledian distance to measure the distance
        for i,point in enumerate(data[:]):
            curr_assigned = 0
            min_distance = eucledian_distance(k_points[0],point)
            j = 1
            while j<k:
                curr_distance = eucledian_distance(k_points[j],point)
                if curr_distance < min_distance:
                    #we have found a closer point
                    min_distance = curr_distance
                    curr_assigned = j
                j += 1
            labels[i] = curr_assigned

        #now update the means
        for i in range(k):
            k_points[i] = np.array([0,0])
        
        n_points = [0 for _ in range(k)]
        for i,point in enumerate(data[:]):
            k_points[labels[i]] += point
            n_points[labels[i]] += 1

        for i in range(k):
            if n_points[i]!=0:
                k_points[i] = np.divide(k_points[i],n_points[i])

    return k_points

def k_means_clustering(k,epochs = 100):
    global labels,data
    k_means = train(k,epochs)
    
    #print the clusters
    for i in range(k):
        print(f"Cluster {i}:")
        for index in range(data.shape[0]):
            if labels[index]==i:
                print(f"{data_point_names[index]}{data[index]} ",end="")
        print()
        print(f"Cluster mean is {k_means[i]}")
        print()


if __name__=="__main__":
    for i in range(1,data.shape[0]+1):
        print(f"-------Taking k = {i}--------")
        k_means_clustering(i)
        print()
