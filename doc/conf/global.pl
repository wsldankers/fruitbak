our %conf;

$conf{rootdir} = '/var/tmp/fruitbak-test-backup';

#$conf{pool} = ['compress'];
#$conf{pool} = ['verify', pool => ['compress', pool => ['encrypt', key => 'k1+iU9J5IdDC7eqAlSFJ1TRo35xSIy0zdOhZYZeRT9I=']]];

$conf{maxjobs} = 2;

1;
