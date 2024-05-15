# GANomaly

This was cloned from a github repo.
I improvised on that version to make getting the resutls out from the model easier.

#This is how we can train this for our own datasets
Training: python train.py --dataset peg_cdsem --save_test_images --niter 15 --isize 128 --nz 125 --lr 0.0002 --w_con 80 --manualseed 42
Testing: python train.py --dataset peg_cdsem --save_test_images --niter 1 --isize 128 --nz 125 --lr 0.0002 --w_con 80 --manualseed 42 --load_weights --batchsize 1

It was important to understand the code in order to make decisions as to where the outputs are generated and how we can save them. 
