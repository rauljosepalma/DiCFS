clear;

nfkb_model;

nfkb_experimental_scheme;

% Experimental data may be introduced as matrices here or can be loaded from a .m, .mat or .xls
% file. Here it is illustrated how the data may be input in two different ways: by means of matrices
% and by loading data from .mat file
%EXPERIMENT 1 : COPY MATRICES FROM RESULTS OF AMIGO_SData('nfkb_psdata')

inputs.exps.exp_data{1}=...
    [1.2669e-002  6.1880e-002  5.4264e-006  1.9070e-001  1.4943e-002  4.5282e-006
    1.0421e-001  1.2293e-002  4.1026e-005  2.0745e-001  8.0195e-002  2.8359e-005
    2.5579e-001  2.8736e-003  8.4920e-005  2.1439e-001  6.5629e-002  5.5768e-005
    2.9200e-001  8.5916e-003  1.2035e-004  2.4198e-001  3.0735e-003  1.5975e-004
    1.5901e-001  5.8807e-002  1.9135e-004  1.7245e-001  6.7410e-003  1.8668e-004
    2.7352e-002  7.1891e-002  1.6524e-004  1.7904e-001  2.4773e-004  2.0223e-004
    3.4334e-003  7.9096e-002  1.1346e-004  2.2031e-001  1.0285e-002  1.2105e-004
    7.8331e-003  6.2006e-002  7.1724e-005  1.9185e-001  7.3342e-003  5.0684e-005
    5.8919e-002  5.4425e-002  8.1396e-005  1.7215e-001  6.1064e-003  5.2071e-005
    8.9897e-002  4.3884e-002  7.4471e-005  2.0622e-001  3.2625e-003  1.0311e-004
    1.1045e-001  4.8631e-002  1.2085e-004  2.3096e-001  5.5002e-003  1.1759e-004
    5.0787e-002  4.2017e-002  1.0379e-004  2.1401e-001  7.1844e-003  1.1820e-004];



%EXPERIMENT 2 : LOADS DATA FROM RESULTS OF AMIGO_SData('nfkb_psdata')
temp=load('psdataNFKB','results');

inputs.exps.exp_data{2}=temp.results.sim.exp_data{2};

inputs.exps.error_data{2}=temp.results.sim.error_data{2};
clear temp;


%==================================
% UNKNOWNS RELATED DATA
inputs.PEsol.id_global_theta='all';

inputs.PEsol.global_theta_guess=inputs.model.par;

inputs.PEsol.global_theta_max=inputs.model.par+inputs.model.par*100;

inputs.PEsol.global_theta_min=inputs.model.par-inputs.model.par*0.01;

inputs.PEsol.global_theta_guess=inputs.PEsol.global_theta_min+...
        (inputs.PEsol.global_theta_max-inputs.PEsol.global_theta_min)...
            .*rand(1,length(inputs.PEsol.global_theta_max));

%==================================
% COST FUNCTION RELATED DATA
%==================================

inputs.PEsol.PEcost_type='lsq';

inputs.nlpsol.iterprint=1;
inputs.nlpsol.cvodes_gradient=1;
inputs.nlpsol.mkl_gradient=0;
inputs.nlpsol.mkl_tol=1e-11;

AMIGO_Prep(inputs);

AMIGO_SModel(inputs);

inputs.model.exe_type='fullC';

inputs.model.odes_file=fullfile(pwd,'fullC.c');

[inputs privstruct]=AMIGO_Prep(inputs);

save('load_C','-v7.3','inputs','privstruct');


