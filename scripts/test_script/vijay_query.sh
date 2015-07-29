#!/usr/local/bin/octave --traditional
printf ("%s", program_name ());
arg_list = argv ();
for i = 1:nargin
  printf (" %s", arg_list{i});
endfor
printf ("\n");
printf ("database %s\n", arg_list{1});
printf ("table %s\n", arg_list{2});
printf ("query %s\n", arg_list{3});
printf ("\n");
disp('hello there!');
disp('test2');
disp('["adam","database"]')
exit;