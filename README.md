# Non-Redundant Dynamic Fragment Allocation with Horizontal Partition in Distributed Database System

Implementation of the Paper: Non-Redundant Dynamic Fragment Allocation with Horizontal Partition in Distributed Database System

## Algorithm

Step 1: Calculate the total number of read and write accesses between the fragment Fi and each remote site Sx that made access to the fragment respectively , 
       If  Fi(Njx < access threshold ), then do nothing, otherwise go to the following step. 
       
Step 2: Calculate the average volume of read and write data transmitted between fragment Fi and all the sites (including local site Sy) that made access to the fragment Fi.

Step 3: Calculate the average volume of read and write data transmitted of each accessing remote site.
	      If Step 3 result < Step 2 result , Then do nothing. 
	      Else go to next step

Step 4: If there is only one accessing remote site Sx qualify constraints stated in Step 3
	      Then, Reallocate fragment fi to remote site Sx and remove from the current site Sy, 
	
        Update Fragment Allocation Information matrix. 

Step 5: If more than one sites qualify constraints stated in step 3 
        Then, Calculate the volume of write data transmitted between the fragment Fi and all 	qualified remote sites within time interval.
	      Finally select the site which has maximum write data volume than other sites.
	
        Reallocate the fragment to that site and update the allocation information matrix.


