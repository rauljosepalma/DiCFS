% $Header$
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% TITLE: The three step pathway by Mendes (2001)
%        Mendes P, 2001. Modeling large biological systems from functional
%        genomic data: Parameter estimation. In Foundations of systems 
%        biology (ed. H. Kitano), pp. 163–186. MIT Press, Cambridge, MA.
%
% MODEL:      
%       dG1dt = V1/(1+(P/Ki1)^ni1+(Ka1/S)^na1)- k_1*G1;
%       dG2dt = V2/(1+(P/Ki2)^ni2+(Ka2/M1)^na2) - k_2*G2;
%       dG3dt = V3/(1+(P/Ki3)^ni3+(Ka3/M2)^na3) - k_3*G3;
%       dE1dt = V4*G1/(K4+G1) - k_4*E1;
%       dE2dt = V5*G2/(K5+G2) - k_5*E2;
%       dE3dt = V6*G3/(K6+G3) - k_6*E3;
%       dM1dt = kcat1*E1*(1/Km1)*(S-M1)/(1+S/Km1+M1/Km2)-kcat2*E2*(1/Km3)*(M1-M2)/(1+M1/Km3+M2/Km4);
%       dM2dt = kcat2*E2*(1/Km3)*(M1-M2)/(1+M1/Km3+M2/Km4)-kcat3*E3*(1/Km5)*(M2-P)/(1+M2/Km5+P/Km6);
%
%        where M1, M2, E1, E2, E3, G1, G2, and G3 represent the concentrations
%        of the species involved in the different biochemical reactions
%        and S and P keep fixed initial values for each experiment
%        (i.e., parameters under our control).
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
%OTHER REFERENCES:
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

%============================
% RESULTS PATHS RELATED DATA
%============================
results.pathd.results_folder='Mendes_3steppath_model'; % Folder to keep results (in Results) for a given problem                       
results.pathd.short_name='Mendes';                       % To identify figures and reports for a given problem 
                                                     % ADVISE: the user may introduce any names related to the problem at hand 
results.pathd.runident='run1';                       % [] Identifier required in order not to overwrite previous results
                                                     %    This may be modified from command line. 'run1'(default)
                                              

%============================
% MODEL RELATED DATA
%============================
%inputs.model.input_model_type='charmodelF';          % Model introduction: 'charmodelF'|'charmodelM'|'matlabmodel'|'sbmlmodel'|'fortranmodel'|                        
inputs.model.input_model_type='charmodelC';                                                %                     'blackboxmodel'|'blackboxcost                   
%inputs.ivpsol.ivpsolver='cvodes';
%inputs.ivpsol.senssolver='cvodes';
inputs.model.n_st=8;                                 % Number of states                                  
inputs.model.n_par=36;                                % Number of model parameters                                  
inputs.model.n_stimulus=2;                           % Number of inputs, stimuli or control variables   
inputs.model.names_type='custom';                    % [] Names given to states/pars/inputs: 'standard' (x1,x2,...p1,p2...,u1, u2,...) 
                                                     %                                       'custom'(default)
inputs.model.st_names=char('G1','G2','G3','E1','E2','E3','M1','M2');  % Names of the states                                         
inputs.model.par_names=char('V1','Ki1','ni1','Ka1','na1','k_1','V2','Ki2',...
          'ni2','Ka2','na2','k_2','V3','Ki3','ni3','Ka3','na3','k_3',...
          'V4', 'K4','k_4','V5','K5','k_5','V6','K6','k_6',...
          'kcat1','Km1','Km2','kcat2','Km3','Km4','kcat3','Km5','Km6');             % Names of the parameters  
inputs.model.stimulus_names=char('S','P');          % Names of the stimuli, inputs or controls                                  
inputs.model.eqns=...                                % Equations describing system dynamics. Time derivatives are regarded 'd'st_name''
            char('dG1=V1/(1+(P/Ki1)^ni1+(Ka1/S)^na1)- k_1*G1',...
                 'dG2= V2/(1+(P/Ki2)^ni2+(Ka2/M1)^na2) - k_2*G2',...
                 'dG3= V3/(1+(P/Ki3)^ni3+(Ka3/M2)^na3) - k_3*G3',...
                 'dE1= V4*G1/(K4+G1) - k_4*E1',...
                 'dE2= V5*G2/(K5+G2) - k_5*E2',...
                 'dE3= V6*G3/(K6+G3) - k_6*E3',...
                 'dM1=kcat1*E1*(1/Km1)*(S-M1)/(1+S/Km1+M1/Km2)-kcat2*E2*(1/Km3)*(M1-M2)/(1+M1/Km3+M2/Km4)',...  
                 'dM2=kcat2*E2*(1/Km3)*(M1-M2)/(1+M1/Km3+M2/Km4)-kcat3*E3*(1/Km5)*(M2-P)/(1+M2/Km5+P/Km6)');
             
inputs.model.par=[  1  1  2  1  2  1  1  1  2  1  2  1  1  1  2  1  2 ...
    1 0.1 1  0.1 0.1  1  0.1 0.1 1 0.1  1  1  1  1  1  1  1  1  1];      % Nominal value for the parameters, this allows to fix known parameters
                                                      % These values may be updated during optimization  
