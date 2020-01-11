from kneed import KneeLocator

x = [2,3,4,5,6,7,8,9,10]
y = [367.77152670800854,271.10371705436893,190.08994176717303,138.94120466193476,122.01468451528916,106.54955421942,88.80304403670353,85.8122857167995]

kneedle = KneeLocator(x,y, S=1.0, curve='convex', direction='decreasing')

print("Elbow at",round(kneedle.elbow, 3))
