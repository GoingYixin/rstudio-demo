age <- c(12,13,12,26,25,11,13,15)
gengder <- c('m','f','m','m','f','f','f','m')
t1 = table(gengder,age)
t2 = data.frame(age,gengder)
print(t1)

