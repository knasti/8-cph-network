# Finding time based on acceleration and velocity
def time_acc(to_velocity, from_velocity, acceleration):
    time = (to_velocity - from_velocity) / acceleration
    return time


# Finding distance based on acceleration and time
def dist_acc(acceleration, time):
    acc_distance = 0.5 * abs(acceleration) * (time * time)
    return acc_distance