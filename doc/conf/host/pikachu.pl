our %conf;

$conf{user} = 'root';
$conf{host} = 'rot';

#$conf{share} = {precommand => q{env}};

#$conf{share} = {transfer => ['rsync', command => q{exec rsh $hostname exec rsync "$@"}]};

$conf{shares} = [
	{name => 'rsync', path => '/var/tmp/afdrukken/extra', transfer => ['rsync', command => q{exec rsync "$@"}]},
	{name => 'local', path => '/var/tmp/afdrukken', transfer => ['local']},
#	{name => 'usr', path => '/usr/include', transfer => ['rsync']},
#	'/usr/src',
#	{name => 'bak', path => '/bak'},
];

#$conf{shares} = ['/var/tmp/afdrukken', '/usr'];
#$conf{shares} = ['/var/tmp/afdrukken/extra'];

1;
