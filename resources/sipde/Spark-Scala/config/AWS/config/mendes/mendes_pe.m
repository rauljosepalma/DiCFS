% $Header$
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% mendes_pe.m
%
% PARAMETER ESTIMATION: To find the 36 unknown parameters from a set of 16
%        experiments performed under different S and P (inputs) conditions
%        and where all states are measured at 21 equidistant sampling 
%        times
%        Parameters are classified in:
%             Hill coefficients: allowed to vary within the range (0.1, 10)
%             and all other parameters allowed to vary within the
%             range (1e-6, 1e3).
%
% REFERENCES:
%      >Moles, C. G., Pedro Mendes and Julio R. Banga (2003) Parameter
%       estimation in biochemical pathways: a comparison of global
%       optimization methods. Genome Research, 13(11):2467-2474 
%      >Rodriguez-Fernandez, M., J. A. Egea and J. R. Banga (2006) Novel
%       Metaheuristic for Parameter Estimation in Nonlinear Dynamic Biological 
%       Systems. BMC Bioinformatics 7:483.
%
% NOTE!!!: [] indicates that the corresponding input may be omitted,
%              default value will be assigned
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
clear;
 mendes_model
 mendes_experimental_scheme
 
%==================================
% EXPERIMENTAL DATA RELATED INFO
%==================================
%inputs.exps.data_type='real';                         % Type of data: 'pseudo'|'pseudo_pos'|'real'             
%inputs.exps.noise_type='homo';                        % Experimental noise type: Homoscedastic: 'homo'|'homo_var'(default) 

load('mendes_exp_data.mat');                          % Reads data from a .mat file
for iexp=1:inputs.exps.n_exp
inputs.exps.exp_data{iexp}=[G1(:,iexp) G2(:,iexp) G3(:,iexp) E1(:,iexp) E2(:,iexp) E3(:,iexp) M1(:,iexp) M2(:,iexp)];
end

%==================================
% UNKNOWNS RELATED DATA
%==================================

% GLOBAL UNKNOWNS (SAME VALUE FOR ALL EXPERIMENTS)

%inputs.PEsol.id_global_theta=char('V1', 'k_1', 'V2', 'k_2', 'V3', 'k_3', 'V4', 'k_4', 'K4', 'K5', ...
%'V5', 'V6', 'k_6', 'kcat1', 'kcat2', 'kcat3');                         % 'all'|User selected 
%inputs.PEsol.global_theta_max=10.*ones(1,16);
%inputs.PEsol.global_theta_min=5e-2.*ones(1,16);       % Minimum allowed values for the parameters

inputs.PEsol.id_global_theta='all';                         % 'all'|User selected 
inputs.PEsol.global_theta_max=[  5e2  5e2 10  5e2 10  5e2  5e2  5e2  10  5e2  10  5e2  5e2 5e2...
  10 5e2 10  5e2  5e2 5e2 5e2 5e2 5e2 5e2 5e2 5e2  5e2  5e2  5e2 5e2  5e2 5e2  5e2 5e2 5e2 5e2];   % Maximum allowed values for the paramters
inputs.PEsol.global_theta_min=[  1e-6   1e-6  1e-1  1e-6  1e-1  1e-6  1e-6  1e-6  1e-1  1e-6  1e-1...
  1e-6  1e-6  1e-6  1e-1  1e-6  1e-1  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6...
  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6  1e-6];       % Minimum allowed values for the parameters

% inputs.PEsol.id_global_theta=char('k_3','na3','na2','k_6','k_2','k_4','k_1','V3','V2','V1','V5','K5');
% inputs.PEsol.global_theta_max=[ 1e3 10 10  1e3 1e3 1e3 1e3 1e3 1e3 1e3 1e3 1e3];
% inputs.PEsol.global_theta_min=[ 1e-6 0.1 0.1 1e-6 1e-6 1e-6 1e-6 1e-6 1e-6 1e-6 1e-6 1e-6];
%inputs.PEsol.global_theta_guess=[ 1 2 2 0.1 9.9991e-1 0.1 1.0001 1.000 9.9991e-001 1.0001e+000 0.1 1];

%values for the parameters
%==================================
% COST FUNCTION RELATED DATA
%==================================
         

inputs.PEsol.PEcost_type='lsq';
inputs.PEsol.lsq_type='Q_expmax';  

%inputs.nlpsol.iterprint=1;
%inputs.nlpsol.cvodes_gradient=1;
%inputs.nlpsol.mkl_gradient=0;
%inputs.nlpsol.mkl_tol=1e-11;

AMIGO_Prep(inputs);

AMIGO_SModel(inputs);

inputs.model.exe_type='fullC';

inputs.model.odes_file=fullfile(pwd,'fullC.c');

[inputs privstruct]=AMIGO_Prep(inputs);

save('load_C','-v7.3','inputs','privstruct');

%save('load_C.mat');

