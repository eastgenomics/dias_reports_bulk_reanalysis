while True:
	conf = input('y or n')
	if conf == 'y':
		print('foo')
		break
	elif conf == 'n':
		print('bar')
		exit()	
	else:
		print('baz')

print('post')
