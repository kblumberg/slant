'use client';

// import { WalletMultiButton } from "@solana/wallet-adapter-react-ui";
import slantRounded from '../../public/images/logos/slant-rounded.png';

const Header = () => {
    return (
        <div className="fixed top-0 left-0 right-0 z-10 w-full text-white font-semibold text-2xl">
            <div className="flex items-center justify-between pt-3 pb-2 px-4 sm:px-6 bg-[#101218] w-full max-w-screen overflow-x-hidden">
                <div className="flex items-center">
                    <img src={slantRounded.src} alt="Slant Logo" className="h-6 w-6 mr-2" />
                    Slant
                </div>
                {/* <div className="walletContainer">
                    <WalletMultiButton />
                </div> */}
            </div>
            <div className="flex items-center pt-2 pl-4 bg-gradient-to-b from-[#101218] to-transparent"></div>
        </div>
    );
};

export default Header;
