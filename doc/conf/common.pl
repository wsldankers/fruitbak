our %conf;

$conf{expiry} = ['or', any => [['logarithmic'], ['age', max => '1d']]];
#$conf{expiry} = ['logarithmic', keep => 3];

$conf{precommand} = sub { warn "starting $ENV{name}\n" };
$conf{postcommand} = sub { warn "finishing $ENV{name}\n" };

1;
