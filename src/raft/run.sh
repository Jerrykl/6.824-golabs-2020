iter=1;

while true;

do 

    go test || break;

    echo "Pass ${iter}-th iteration" 

    ((iter++));

done; 

echo "The program fails at the ${iter}-th iteration";
