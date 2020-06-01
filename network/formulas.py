# Finding time based on acceleration and velocity
def time_acc(to_velocity, from_velocity, acceleration):
    return (to_velocity - from_velocity) / acceleration


# Finding distance based on acceleration and time
def dist_acc(acceleration, time):
    return 0.5 * abs(acceleration) * (time * time)